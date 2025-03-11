import os
from typing import Any

from airfunctions.context import ContextManager


class TerraformContextManager(ContextManager):
    """Context manager for Terraform blocks"""
#    _mapping = {}
#
#    def push_context_obj(cls, obj: Any) -> None:
#        cls._mapping[obj.name] = obj


class Local:
    """Class for Terraform local values"""

    def __getattribute__(self, name: str) -> Any:
        return ref(f"local.{name}")
        return TerraformContextManager._mapping[name]


local = Local()


class Ref:
    """Class for Terraform references"""

    def __init__(self, path: str):
        self.path = path

    def __str__(self):
        return f"{self.path}"


ref = Ref


class TerraformFunction:
    """Class for Terraform function blocks"""

    def __init__(self, name):
        self.name = name
        self.value = None

    def __str__(self):
        return f"TerraformFunction(name={self.name}, value={self.value})"

    def __call__(self, *args: Any, **kwds: Any) -> str:
        _kwds = []
        _args = []

        for arg in args:
            if isinstance(arg, TerraformFunction):
                _args.append(arg.value)
            else:
                _args.append(TerraformBlock._format_value(arg))

        for k, v in kwds.items():
            if isinstance(v, TerraformFunction):
                _kwds.append(f'{k} = {v.value}')
            else:
                _kwds.append(f'{k} = {TerraformBlock._format_value(v)}')
        
        _args_str = ', '.join([str(arg) for arg in _args])
        _kwds_str = ', '.join(_kwds)

        if _args_str and _kwds_str:
            self.value = f"{self.name}({_args_str}, {_kwds_str})"
        else:
            self.value = f"{self.name}({_args_str or _kwds_str})"
        return self


def format(*args, **kwds):
    return TerraformFunction("format")(*args, **kwds)


def templatefile(path: str, map: Any):
    return TerraformFunction("templatefile")(path, map)


def jsonencode(value: dict):
    return TerraformFunction("jsonencode")(value)

def filemd5(path: str):
    return TerraformFunction("filemd5")(path)

class TerraformBlock:
    """Base class for all Terraform blocks"""

    def __init__(self, **kwargs):
        self.attributes = kwargs
        self.blocks = []
        TerraformContextManager().push_context_obj(self)

    def ref(self, attr_name: str) -> Ref:
        if hasattr(self, 'block_name'):
            return ref(f"{self.block_type}.{self.block_name}.{attr_name}")
        elif getattr(self, 'block_type', None) == "data":
            return ref(f"{self.block_type}.{self.resource_name}.{attr_name}")
        elif hasattr(self, 'resource_name'):
            return ref(f"{self.resource_type}.{self.resource_name}.{attr_name}")
        else:
            return ref(f"{self.block_type}.{attr_name}")

    def add_block(self, block):
        """Add a nested block to this block"""
        self.blocks.append(block)
        return self

    def to_string(self, indent=0):
        """Convert the block to a Terraform configuration string"""
        lines = []
        indent_str = "  " * indent

        # Handle different block types
        if hasattr(self, 'resource_type') and hasattr(self, 'resource_name'):
            lines.append(
                f"{indent_str}{self.block_type} \"{self.resource_type}\" \"{self.resource_name}\" {{")
        elif hasattr(self, 'block_name'):
            if self.block_name:
                lines.append(
                    f"{indent_str}{self.block_type} \"{self.block_name}\" {{")
            else:
                lines.append(f"{indent_str}{self.block_type} {{")
        else:
            lines.append(f"{indent_str}{self.block_type} {{")

        # Add attributes
        for key, value in self.attributes.items():
            lines.append(f"{indent_str}  {key} = {self._format_value(value)}")

        # Add nested blocks
        for block in self.blocks:
            lines.append(block.to_string(indent + 1))

        lines.append(f"{indent_str}}}")
        return "\n".join(lines)

    @classmethod
    def _format_value(cls, value):
        """Format a value according to Terraform HCL syntax"""
        if isinstance(value, str):
            # Check if the string is a reference or an expression that shouldn't be quoted
            if (value.startswith("${") and value.endswith("}")) or \
               any(value.startswith(prefix) for prefix in ["var.", "local.", "module.", "data."]):
                return value
            return f"\"{value}\""
        elif isinstance(value, Ref):
            return str(value)
        elif isinstance(value, TerraformFunction):
            return value.value
        elif isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, list):
            elements = [cls._format_value(elem) for elem in value]
            return f"[{', '.join(elements)}]"
        elif isinstance(value, dict):
            pairs = [
                f"{k} = {cls._format_value(v)}" for k, v in value.items()
            ]
            return f"{{{', '.join(pairs)}}}"
        elif value is None:
            return "null"
        else:
            return str(value)


class ConfigBlock(TerraformBlock):
    """Class for generic configuration blocks"""

    def __init__(self, block_type, block_name=None, **kwargs):
        super().__init__(**kwargs)
        self.block_type = block_type
        self.block_name = block_name

    @classmethod
    def nested(cls, block_type, **kwargs):
        """Create a nested configuration block without a name"""
        return cls(block_type, None, **kwargs)


class Resource(TerraformBlock):
    """Class for Terraform resource blocks"""

    def __init__(self, resource_type, resource_name, **kwargs):
        super().__init__(**kwargs)
        self.block_type = "resource"
        self.resource_type = resource_type
        self.resource_name = resource_name


class Data(TerraformBlock):
    """Class for Terraform data source blocks"""

    def __init__(self, data_type, data_name, **kwargs):
        super().__init__(**kwargs)
        self.block_type = "data"
        self.resource_type = data_type
        self.resource_name = data_name


class Module(TerraformBlock):
    """Class for Terraform module blocks"""

    def __init__(self, module_name, **kwargs):
        super().__init__(**kwargs)
        self.block_type = "module"
        self.block_name = module_name


class Variable(TerraformBlock):
    """Class for Terraform variable blocks"""

    def __init__(self, var_name, type=None, default=None, description=None, **kwargs):
        attributes = kwargs
        if type is not None:
            attributes["type"] = type
        if default is not None:
            attributes["default"] = default
        if description is not None:
            attributes["description"] = description
        super().__init__(**attributes)
        self.block_type = "variable"
        self.block_name = var_name


class Output(TerraformBlock):
    """Class for Terraform output blocks"""

    def __init__(self, output_name, value, description=None, **kwargs):
        attributes = {"value": value}
        if description is not None:
            attributes["description"] = description
        attributes.update(kwargs)
        super().__init__(**attributes)
        self.block_type = "output"
        self.block_name = output_name


class Locals(TerraformBlock):
    """Class for Terraform locals block"""

    def __init__(self, **locals_dict):
        super().__init__(**locals_dict)
        self.block_type = "locals"
        self.block_name = None


class Provider(TerraformBlock):
    """Class for Terraform provider blocks"""

    def __init__(self, provider_name, **kwargs):
        super().__init__(**kwargs)
        self.block_type = "provider"
        self.block_name = provider_name


class TerraformConfig(TerraformBlock):
    """Class for Terraform config blocks"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.block_type = "terraform"
        self.block_name = None


class Backend(TerraformBlock):
    """Class for Terraform backend blocks"""

    def __init__(self, backend_type, **kwargs):
        super().__init__(**kwargs)
        self.block_type = "backend"
        self.block_name = backend_type


class Required_Providers(TerraformBlock):
    """Class for Terraform required_providers blocks"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.block_type = "required_providers"
        self.block_name = None


class Provider_Config(TerraformBlock):
    """Class for provider configuration within required_providers"""

    def __init__(self, provider_name, **kwargs):
        super().__init__(**kwargs)
        self.block_type = provider_name
        self.block_name = None


class DynamicBlock(TerraformBlock):
    """Class for Terraform dynamic blocks"""

    def __init__(self, block_name, iterator=None, **kwargs):
        super().__init__(**kwargs)
        self.block_type = "dynamic"
        self.block_name = block_name
        if iterator:
            self.attributes["iterator"] = iterator


class Content(TerraformBlock):
    """Class for content blocks within dynamic blocks"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.block_type = "content"
        self.block_name = None


class TerraformConfiguration:
    """Class for a complete Terraform configuration"""

    def __init__(self):
        self.blocks = []

    def add(self, block):
        """Add a block to the configuration"""
        self.blocks.append(block)
        return self

    def to_string(self):
        """Convert the entire configuration to a Terraform configuration string"""
        return "\n\n".join(block.to_string() for block in self.blocks)

    def save(self, filename):
        """Save the configuration to a file"""
        with open(filename, 'w') as f:
            f.write(self.to_string())


class TerraformBlocksCollection:
    """Class for a collection of Terraform blocks"""

    def __init__(self, blocks: list[TerraformBlock] = None):
        self.blocks = blocks or []

    def add(self, block: TerraformBlock):
        """Add a block to the collection"""
        self.blocks.append(block)
        return self

    def to_string(self):
        """Convert the collection to a Terraform configuration string"""
        return "\n\n".join(block.to_string() for block in self.blocks)

    def save(self, filename):
        """Save the collection to a file"""
        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as f:
            f.write(self.to_string())


# Example usage
if __name__ == "__main__":
    # Create a new Terraform configuration
    tf = TerraformConfiguration()

    # Add terraform configuration block
    terraform_config = TerraformConfig(
        required_version=">=1.3.0"
    )

    # Add required providers
    required_providers = Required_Providers()

    # Add provider configurations to required_providers
    aws_provider_config = Provider_Config("aws",
                                          source="hashicorp/aws",
                                          version="~> 4.0")
    required_providers.add_block(aws_provider_config)

    # Add backend configuration
    backend = Backend("s3",
                      bucket="my-terraform-state",
                      key="example/terraform.tfstate",
                      region="us-west-2",
                      encrypt=True)

    # Add the backend to the terraform configuration block
    terraform_config.add_block(backend)
    terraform_config.add_block(required_providers)

    # Add the terraform configuration block to the main configuration
    tf.add(terraform_config)

    # Add provider
    provider = Provider("aws",
                        region="us-west-2",
                        profile="default")
    tf.add(provider)

    # Add variables
    vpc_cidr = Variable("vpc_cidr",
                        type="string",
                        default="10.0.0.0/16",
                        description="CIDR block for the VPC")
    tf.add(vpc_cidr)

    account_id = Variable("account_id",
                          type="string",
                          description="AWS Account ID")
    tf.add(account_id)

    provider_name = Variable("provider_name",
                             type="string",
                             description="SAML provider name")
    tf.add(provider_name)

    trusted_role_arn = Variable("trusted_role_arn",
                                type="string",
                                description="Trusted role ARN")
    tf.add(trusted_role_arn)

    # Example of IAM policy document with nested blocks
    # This demonstrates how to create complex nested configurations
    iam_policy = Data("aws_iam_policy_document",
                      "event_stream_bucket_role_assume_role_policy")

    # Create a statement block with attributes and nested blocks
    statement = ConfigBlock.nested("statement",
                                   actions=["sts:AssumeRole"])

    # Add service principal nested block to statement
    service_principal = ConfigBlock.nested("principals",
                                           type="Service",
                                           identifiers=["firehose.amazonaws.com"])
    statement.add_block(service_principal)

    # Add AWS principal nested block to statement
    aws_principal = ConfigBlock.nested("principals",
                                       type="AWS",
                                       identifiers=["${var.trusted_role_arn}"])
    statement.add_block(aws_principal)

    # Add Federated principal nested block to statement
    federated_principal = ConfigBlock.nested("principals",
                                             type="Federated",
                                             identifiers=[
                                                 "arn:aws:iam::${var.account_id}:saml-provider/${var.provider_name}",
                                                 "cognito-identity.amazonaws.com"
                                             ])
    statement.add_block(federated_principal)

    # Add the statement block to the IAM policy
    iam_policy.add_block(statement)

    # Add a second statement to demonstrate multiple blocks of the same type
    statement2 = ConfigBlock.nested("statement",
                                    actions=["s3:GetObject", "s3:PutObject"],
                                    resources=["arn:aws:s3:::example-bucket/*"])

    # Add condition block to the second statement
    condition = ConfigBlock.nested("condition",
                                   test="StringEquals",
                                   variable="aws:SourceAccount",
                                   values=["${var.account_id}"])
    statement2.add_block(condition)

    # Add the second statement block to the IAM policy
    iam_policy.add_block(statement2)

    # Add the IAM policy to the main configuration
    tf.add(iam_policy)

    # Add an S3 bucket resource
    s3_bucket = Resource("aws_s3_bucket", "example",
                         bucket="my-example-bucket",
                         force_destroy=True)

    # Add server_side_encryption_configuration nested block
    encryption = ConfigBlock.nested("server_side_encryption_configuration")

    # Add rule nested block within encryption
    encryption_rule = ConfigBlock.nested("rule")

    # Add apply_server_side_encryption_by_default nested block within rule
    encryption_default = ConfigBlock.nested("apply_server_side_encryption_by_default",
                                            sse_algorithm="AES256")

    # Build the nested structure
    encryption_rule.add_block(encryption_default)
    encryption.add_block(encryption_rule)
    s3_bucket.add_block(encryption)

    # Add the S3 bucket to the main configuration
    tf.add(s3_bucket)

    # Print the configuration
    print(tf.to_string())

    # Save the configuration to a file
    # tf.save("main.tf")

    # EXAMPLE COMMENT:
    """
    # Creating a data block with multiple nested blocks:
    
    # 1. Create the main data block
    iam_policy = Data("aws_iam_policy_document", "example")
    
    # 2. Create a statement block with attributes
    statement = ConfigBlock.nested("statement", 
                                  effect = "Allow",
                                  actions = ["s3:GetObject"])
    
    # 3. Create a principals block within the statement
    principals = ConfigBlock.nested("principals",
                                   type = "AWS",
                                   identifiers = ["arn:aws:iam::123456789012:role/example"])
    
    # 4. Add the principals block to the statement
    statement.add_block(principals)
    
    # 5. Add the statement block to the iam_policy
    iam_policy.add_block(statement)
    
    # 6. Add the iam_policy to the configuration
    tf.add(iam_policy)
    """
