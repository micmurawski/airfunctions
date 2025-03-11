import json
import os


class Config:
    _instance = None

    def __new__(cls):
        """
        Override the __new__ method to ensure only one instance is created.
        """
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._initialize_defaults()
        return cls._instance

    def _initialize_defaults(self):
        """
        Set up default values for all configuration settings.
        """
        self.resource_prefix = ""
        self.resource_suffix = ""
        self.terraform_dir = os.environ.get("TERRAFORM_DIR", "./terraform")
        self.environment = "dev"
        self.lambda_module_version = "7.20.1"
        self.lambda_module_source = "terraform-aws-modules/lambda/aws"
        self.aws_region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"

    def reset(self):
        self._initialize_defaults()

    def load_from_file(self, filepath):
        """
        Load configuration from a file (JSON, YAML, etc.).

        This is a placeholder - you would implement file loading logic based on your needs.
        """
        with open(filepath, 'r') as f:
            config_data = json.load(f)
            for key, value in config_data.items():
                if hasattr(self, key):
                    setattr(self, key, value)

    def save_to_file(self, filepath):
        """
        Save current configuration to a file.

        This is a placeholder - you would implement file saving logic based on your needs.
        """
        json.dump(self.__dict__, open(filepath, "w"))
