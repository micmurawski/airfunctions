class TerraformBlock:
    """Base class for all Terraform blocks"""
    def __init__(self, block_name=None, **kwargs):
        self.block_name = block_name
        self.attributes = kwargs
        self.blocks = []

    def add_block(self, block):
        """Add a nested block to this block"""
        self.blocks.append(block)
        return self

    def to_string(self, indent=0):
        """Convert the block to a Terraform configuration string"""
        lines = []
        indent_str = "  " * indent
        
        # Special handling for variables and outputs that have a different format
        if isinstance(self, Variable) or isinstance(self, Output):
            lines.append(f"{indent_str}{self.block_type} \"{self.block_name}\" {{")
            for key, value in self.attributes.items():
                lines.append(f"{indent_str}  {key} = {self._format_value(value)}")
            lines.append(f"{indent_str}}}")
            return "\n".join(lines)
        
        # Regular blocks with type and name
        if self.block_name:
            lines.append(f"{indent_str}{self.block_type} \"{self.block_name}\" {{")
        elif hasattr(self, 'resource_type'):
            lines.append(f"{indent_str}{self.block_type} \"{self.resource_type}\" \"{self.resource_name}\" {{")
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
            pairs = [f"{self._format_value(k)} = {self._format_value(v)}" for k, v in value.items()]
            return f"{{{', '.join(pairs)}}}"
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
    def __init__(self, block_name, **kwargs):
        super().__init__(block_name, **kwargs)
        self.block_type = "module"


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
    def __init__(self, workspace=None):
        self.workspace = workspace
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
    # Create a new Terraform configuration
    tf = TerraformConfiguration()
    
    # Add provider
    provider = Provider("aws", region="us-west-2")
    tf.add(provider)
    
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
    
    # Add a resource
    vpc = Resource("aws_vpc", "main",
                  cidr_block="var.vpc_cidr",
                  tags="${local.common_tags}")
    tf.add(vpc)
    
    # Add a data source
    availability_zones = Data("aws_availability_zones", "available",
                             state="available")
    tf.add(availability_zones)
    
    # Add a module
    module = Module("vpc",
                   source="terraform-aws-modules/vpc/aws",
                   version="3.14.0",
                   name="my-vpc",
                   cidr="var.vpc_cidr",
                   azs=["us-west-2a", "us-west-2b", "us-west-2c"],
                   tags="${local.common_tags}")
    tf.add(module)
    
    # Add an output
    vpc_id = Output("vpc_id",
                   value="module.vpc.vpc_id",
                   description="The ID of the VPC")
    tf.add(vpc_id)
    
    # Print the configuration
    print(tf.to_string())
    
    # Save the configuration to a file
    # tf.save("main.tf")