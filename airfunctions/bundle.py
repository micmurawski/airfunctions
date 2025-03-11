import json
import os
import subprocess
import sys
from pathlib import Path

from airfunctions.config import Config
from airfunctions.poetry_utils import get_lambda_build_config
from airfunctions.steps import *
from airfunctions.terrapy import (Backend, ConfigBlock, Data, Locals, Module,
                                  Output, Provider, Provider_Config,
                                  Required_Providers, Resource,
                                  TerraformBlocksCollection, TerraformConfig,
                                  TerraformConfiguration, Variable, filemd5)
from airfunctions.terrapy import format as tf_format
from airfunctions.terrapy import local, ref, templatefile

PYTHON_RUNTIME = f"python{sys.version_info.major}.{sys.version_info.minor}"
LAMBDA_MODULE_VERSION = Config().lambda_module_version
LAMBDA_MODULE_SOURCE = Config().lambda_module_source


def save_dict_to_json_file(data: dict, file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        json.dump(data, f)


class TerraformBundler:
    def __init__(self):
        self.tasks = []
        self.lambda_functions = {}
        self.state_machines = {}

    def validate(self):
        self.collect_resources()
        self.to_terraform()
        cwd = Config().terraform_dir
        subprocess.run(["terraform", "init", "-backend=false"], cwd=cwd)
        subprocess.run(["terraform", "validate"], cwd=cwd)

    def apply(self):
        self.collect_resources()
        self.to_terraform()
        self.build_lambdas()
        self.terraform_apply()

    def terraform_apply(self):
        cwd = Config().terraform_dir
        subprocess.run(["terraform", "init"], cwd=cwd)
        subprocess.run(["terraform", "plan", "-out=plan.out"], cwd=cwd)
        subprocess.run(
            ["terraform", "apply", "-auto-approve", "plan.out"], cwd=cwd)
        os.remove(os.path.join(cwd, "plan.out"))

    def build_lambdas(self):
        process = subprocess.run(
            [sys.executable, "-m", "poetry", "build-lambda"])
        print(process)

    def collect_resources(self):
        self.lambda_functions = LambdaTaskContext._context
        self.state_machines = StateMachineContext._context

    def to_terraform(self):
        """Convert collected resources to Terraform configurations"""
        project_path = Path(".")
        lambda_config = get_lambda_build_config(project_path)
        backend = TerraformBlocksCollection()
        main = TerraformBlocksCollection()
        data = TerraformBlocksCollection()
        locals = TerraformBlocksCollection()

        terraform_config = TerraformConfig()
        required_providers = ConfigBlock.nested(
            "required_providers", aws=dict(source="hashicorp/aws")
        )
        terraform_config.add_block(required_providers)
        backend_config = Backend(
            "local",
            # bucket="my-terraform-state",
            # key="example/terraform.tfstate",
            # region=airfunctions_config.aws_region,
            # encrypt=True,
        )

        terraform_config.add_block(backend_config)
        provider = Provider("aws", region="us-east-1")

        backend.add(terraform_config)
        backend.add(provider)

        # Add provider

        aws_caller_identity = Data("aws_caller_identity", "caller_identity")
        aws_region = Data("aws_region", "region")
        data.add(aws_caller_identity)
        data.add(aws_region)

        bucket = Resource(
            "aws_s3_bucket",
            "assets_bucket",
            bucket=tf_format("%s%s-bucket%s", local.prefix,
                             "assets", local.suffix),
            force_destroy=True,
            tags=local.tags,
        )
        lambda_layer_artifact_path = ".." / \
            Path(lambda_config["layer-artifact-path"])
        aws_s3_bucket_object = Resource(
            "aws_s3_object",
            "lambda_layer_code",
            bucket=bucket.ref("id"),
            key="lambda_layer_code.zip",
            source=str(lambda_layer_artifact_path),
            source_hash=filemd5(str(lambda_layer_artifact_path)),
        )
        main.add(aws_s3_bucket_object)
        main.add(bucket)
        lambda_layer = Module(
            "lambda_layer",
            source=LAMBDA_MODULE_SOURCE,
            version=LAMBDA_MODULE_VERSION,
            create_layer=True,
            layer_name=tf_format(
                "%s-%slayer%s", local.prefix, "lambda", local.suffix),
            compatible_runtimes=[PYTHON_RUNTIME],
            create_package=False,
            s3_existing_package={
                "bucket": bucket.ref("id"),
                "key": aws_s3_bucket_object.ref("key"),
            },
        )
        # templatefile(x, y)
        main.add(lambda_layer)

        # Create Lambda function resources
        lambda_arns = []
        for lambda_task in self.lambda_functions:
            function_artifact_path = ".." / Path(
                lambda_config["function-artifact-path"]
            )
            aws_s3_bucket_object = Resource(
                "aws_s3_object",
                "lambda_function_code_{}".format(lambda_task.name),
                bucket=bucket.ref("id"),
                key="{}.zip".format(lambda_task.name),
                source=str(function_artifact_path),
                source_hash=filemd5(str(function_artifact_path)),
            )
            main.add(aws_s3_bucket_object)
            lambda_module = Module(
                lambda_task.name,
                source=LAMBDA_MODULE_SOURCE,
                version=LAMBDA_MODULE_VERSION,
                tags=local.tags,
                timeout=lambda_task.timeout,
                memory_size=lambda_task.memory_size,
                tracing_mode=lambda_task.tracing_mode,
                handler=lambda_task.handler_path,
                create_role=True,
                attach_network_policy=False,
                attach_cloudwatch_logs_policy=True,
                attach_tracing_policy=True,
                runtime=PYTHON_RUNTIME,
                create_package=False,
                s3_existing_package={
                    "bucket": bucket.ref("id"),
                    "key": aws_s3_bucket_object.ref("key"),
                },
                function_name=tf_format(
                    "%s%s%s", local.prefix, lambda_task.name, local.suffix
                ),
                layers=[lambda_layer.ref("lambda_layer_arn")],
                environment_variables={},
            )
            main.add(lambda_module)
            lambda_arns.append(lambda_module.ref("lambda_function_arn"))

        # Create Step Function State Machine resources
        for state_machine in self.state_machines:
            state_machine_definition_path = Path(
                f"terraform/state_machines/{state_machine.name}.json"
            )

            save_dict_to_json_file(
                state_machine.definition, state_machine_definition_path
            )
            iam_assume_role_policy_document = Data(
                "aws_iam_policy_document",
                "role_assume_role_policy_{}".format(state_machine.name),
            )
            statement = ConfigBlock.nested(
                "statement", actions=["sts:AssumeRole"])

            service_principal = ConfigBlock.nested(
                "principals", type="Service", identifiers=["states.amazonaws.com"]
            )
            statement.add_block(service_principal)
            iam_assume_role_policy_document.add_block(statement)

            iam_role_policy_document = Data(
                "aws_iam_policy_document",
                "role_role_policy_{}".format(state_machine.name),
            )
            statement_1 = ConfigBlock.nested(
                "statement", actions=["lambda:InvokeFunction"], resources=["*"]
            )
            statement_2 = ConfigBlock.nested(
                "statement", actions=["states:StartExecution"], resources=["*"]
            )
            iam_role_policy_document.add_block(statement_1)
            iam_role_policy_document.add_block(statement_2)

            data.add(iam_assume_role_policy_document)
            data.add(iam_role_policy_document)

            aws_iam_role = Resource(
                "aws_iam_role",
                "role_{}".format(state_machine.name),
                name=tf_format(
                    "%s%s%s", local.prefix, state_machine.name, local.suffix
                ),
                assume_role_policy=iam_assume_role_policy_document.ref("json"),
            )
            aws_iam_role_policy = Resource(
                "aws_iam_role_policy",
                "role_policy_{}".format(state_machine.name),
                name=tf_format(
                    "%s%s%s", local.prefix, state_machine.name, local.suffix
                ),
                policy=iam_role_policy_document.ref("json"),
                role=aws_iam_role.ref("id"),
            )
            main.add(aws_iam_role)
            main.add(aws_iam_role_policy)

            state_machine_resource = Resource(
                "aws_sfn_state_machine",
                state_machine.name,
                name=tf_format(
                    "%s%s%s", local.prefix, state_machine.name, local.suffix
                ),
                role_arn=aws_iam_role.ref("arn"),
                definition=templatefile(
                    f"${{path.module}}/{os.path.join(*state_machine_definition_path.parts[1:])}",
                    {
                        "AWS_ACCOUNT_ID": aws_caller_identity.ref("account_id"),
                        "AWS_REGION": aws_region.ref("name"),
                        "prefix": local.prefix,
                        "suffix": local.suffix,
                    },
                ),
            )
            main.add(state_machine_resource)

        locals_block = Locals(
            prefix=Config().resource_prefix,
            suffix=Config().resource_suffix,
            environment=Config().environment,
            lambda_arns=lambda_arns,
            tags={},
        )
        locals.add(locals_block)

        backend.save("terraform/backend.tf")
        data.save("terraform/data.tf")
        locals.save("terraform/locals.tf")
        main.save("terraform/main.tf")
        data.save("terraform/data.tf")
        subprocess.run(["terraform", "fmt", "--recursive"], cwd="./terraform")


if __name__ == "__main__":
    con1 = (step_1.output("a") == 10) | (step_1.output("b") == 20)
    branch_1 = (
        step_1
        >> Pass("pass1")
        >> Choice("Choice#1", default=step_2).choose(con1, step_3)
    )
    branch_1 = branch_1["Choice#1"].choice() >> Pass("next")
    branch_2 = (
        step_4
        >> [
            step_5,
            step_6 >> step_7,
        ]
        >> Pass("pass2", input_path="$[0]", result={"output.$": "$.a"})
    )
    branch = branch_1 >> branch_2
    branch.to_statemachine("example-1")

    Config().resource_prefix = "micmur-test-"
    bundler = TerraformBundler()
    bundler.validate()
