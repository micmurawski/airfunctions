class TerraformBlock:
    """Base class for all Terraform blocks"""

    def __init__(self, name=None, **kwargs):
        self.name = name
        self.attributes = kwargs
        self.blocks = []
        self.config_blocks = {}

    def add_block(self, block):
        """Add a nested block to this block"""
        self.blocks.append(block)
        return self

    def add_config_block(self, block_name, **kwargs):
        """Add a configuration block (like 'lifecycle' or 'timeouts')"""
        if block_name not in self.config_blocks:
            self.config_blocks[block_name] = {}
        self.config_blocks[block_name].update(kwargs)
        return self

    def to_string(self, indent=0):
        """Convert the block to a Terraform configuration string"""
        lines = []
        indent_str = "  " * indent

        # Special handling for variables and outputs that have a different format
        if isinstance(self, Variable) or isinstance(self, Output):
            lines.append(f"{indent_str}{self.block_type} \"{self.name}\" {{")
            for key, value in self.attributes.items():
                lines.append(
                    f"{indent_str}  {key} = {self._format_value(value)}")
            lines.append(f"{indent_str}}}")
            return "\n".join(lines)

        # Regular blocks with type and name
        if self.name:
            lines.append(f"{indent_str}{self.block_type} \"{self.name}\" {{")
        elif hasattr(self, 'resource_type'):
            lines.append(
                f"{indent_str}{self.block_type} \"{self.resource_type}\" \"{self.resource_name}\" {{")
        else:
            lines.append(f"{indent_str}{self.block_type} {{")

        # Add attributes
        for key, value in self.attributes.items():
            lines.append(f"{indent_str}  {key} = {self._format_value(value)}")

        # Add nested blocks
        for block in self.blocks:
            lines.append(block.to_string(indent + 1))

        # Add configuration blocks
        for block_name, block_attrs in self.config_blocks.items():
            lines.append(f"{indent_str}  {block_name} {{")
            for key, value in block_attrs.items():
                lines.append(
                    f"{indent_str}    {key} = {self._format_value(value)}")
            lines.append(f"{indent_str}  }}")

        lines.append(f"{indent_str}}}")
        return "\n".join(lines)

    def _format_value(self, value):
        """Format a value according to Terraform HCL syntax"""
        if isinstance(value, str):
            # Check if the string is a reference or an expression that shouldn't be quoted
            if (value.startswith("${") and value.endswith("}")) or \
               value.startswith("var.") or \
               value.startswith("local.") or \
               value.startswith("module."):
                return value
            return f"\"{value}\""
        elif isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, list):
            elements = [self._format_value(elem) for elem in value]
            return f"[{', '.join(elements)}]"
        elif isinstance(value, dict):
            pairs = [
                f"{k} = {self._format_value(v)}" for k, v in value.items()]
            return f"{{{' '.join(pairs)}}}"
        elif value is None:
            return "null"
        else:
            return str(value)


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
        self.name = module_name


class Variable(TerraformBlock):
    """Class for Terraform variable blocks"""

    def __init__(self, name, type=None, default=None, description=None, **kwargs):
        attributes = kwargs
        if type is not None:
            attributes["type"] = type
        if default is not None:
            attributes["default"] = default
        if description is not None:
            attributes["description"] = description
        super().__init__(name, **attributes)
        self.block_type = "variable"


class Output(TerraformBlock):
    """Class for Terraform output blocks"""

    def __init__(self, name, value, description=None, **kwargs):
        attributes = {"value": value}
        if description is not None:
            attributes["description"] = description
        attributes.update(kwargs)
        super().__init__(name, **attributes)
        self.block_type = "output"


class Locals(TerraformBlock):
    """Class for Terraform locals block"""

    def __init__(self, **locals_dict):
        super().__init__(**locals_dict)
        self.block_type = "locals"


class Provider(TerraformBlock):
    """Class for Terraform provider blocks"""

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.block_type = "provider"


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


# Example usage
if __name__ == "__main__":
    tf = TerraformConfiguration()
    provider = Provider("aws", region="us-west-2")
    tf.add(provider)

    terraform_block = TerraformBlock()
    terraform_block.block_type = "terraform"
    terraform_block.add_config_block("required_providers",
                                     aws={
                                         "source": "hashicorp/aws",
                                         "version": ">= 4.0.0"
                                     }
                                     )
    terraform_block.add_config_block("backend",
                                     s3={
                                         "bucket": "my-terraform-state",
                                         "key": "example/terraform.tfstate",
                                         "region": "us-west-2"
                                     }
                                     )
    tf.add(terraform_block)
    lambda_module = Module(
        "lambda_1",
        source="terraform-aws-modules/lambda/aws",
        version="6.0.1",
        function_name="step_1",
        tags={},
        timeout=900,
        memory_size=256,
        runtime="python3.12",
        tracing_mode="Active",
        create_package=False,
        s3_existing_package={
            "bucket": "bucket-1",
            "key": "key-1"
        },
        layers=[],
        environment_variables={},
        create_role=True,
        attach_network_policy=True,
        attach_cloudwatch_logs_policy=True,
        attach_tracing_policy=True,
    )
    # Add variables
    vpc_cidr = Variable("vpc_cidr",
                        type="string",
                        default="10.0.0.0/16",
                        description="CIDR block for the VPC")
    tf.add(vpc_cidr)

    # Add locals
    locals_block = Locals(
        common_tags={
            "Project": "Example",
            "Environment": "var.environment"
        },
        vpc_name="example-vpc"
    )
    tf.add(locals_block)

    # Add a resource with configuration blocks
    vpc = Resource("aws_vpc", "main",
                   cidr_block="var.vpc_cidr",
                   tags="${local.common_tags}")

    # Add lifecycle configuration block
    vpc.add_config_block("lifecycle",
                         create_before_destroy=True,
                         prevent_destroy=True)

    # Add timeouts configuration block
    vpc.add_config_block("timeouts",
                         create="60m",
                         delete="2h")

    tf.add(vpc)

    # Add a data source
    availability_zones = Data("aws_availability_zones", "available",
                              state="available")
    tf.add(availability_zones)

    # Add a module
    module = Module("vpc",
                    source="terraform-aws-modules/vpc/aws",
                    version="3.14.0",
                    cidr="var.vpc_cidr",
                    azs=["us-west-2a", "us-west-2b", "us-west-2c"],
                    tags="${local.common_tags}")
    tf.add(module)

    # Add an output
    vpc_id = Output("vpc_id",
                    value="module.vpc.vpc_id",
                    description="The ID of the VPC")
    tf.add(vpc_id)
    tf.add(lambda_module)

    # Print the configuration
    print(tf.to_string())

    # Save the configuration to a file
    # tf.save("main.tf")
