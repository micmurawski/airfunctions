import sys

from airfunctions.config import AirFunctionsConfig
from airfunctions.steps import *
from airfunctions.terrapy import (ConfigBlock, Data, Locals, Module, Output,
                                  Provider, Resource,
                                  TerraformBlocksCollection, Variable)
from airfunctions.terrapy import format as tf_format
from airfunctions.terrapy import local, ref


@lambda_task
def step_1(event, context):
    return event


@lambda_task
def step_2(event, context):
    return event


@lambda_task
def step_3(event, context):
    return event


@lambda_task
def step_4(event, context):
    return event


@lambda_task
def step_5(event, context):
    return event


@lambda_task
def step_6(event, context):
    return event


@lambda_task
def step_7(event, context):
    return event


class Bundler:
    def __init__(self):
        self.tasks = []
        self.lambda_functions = {}
        self.state_machines = {}

    def add_task(self, task):
        self.tasks.append(task)

    def collect_resources(self):
        self.lambda_functions = LambdaTaskContext._context
        self.state_machines = StateMachineContext._context

    def to_terraform(self):
        """Convert collected resources to Terraform configurations"""
        main = TerraformBlocksCollection()
        data = TerraformBlocksCollection()
        locals = TerraformBlocksCollection()

        bucket = Resource(
            "aws_s3_bucket",
            "assets_bucket",
            bucket=tf_format("%s-%s-bucket-%s", local.prefix, "assets", local.suffix),
            force_destroy=True,
            tags=ref("local.tags")
        )
        main.add(bucket)
        lambda_layer = Module(
            "lambda_layer",
            source="terraform-aws-modules/lambda/aws",
            version="6.0.1",
            create_layer=True,
            layer_name=tf_format("%s-%s-layer-%s", local.prefix, "lambda", local.suffix),
            compatible_runtimes=[
                f"python{sys.version_info.major}.{sys.version_info.minor}"
            ],
            create_package=False,
            s3_existing_package={}
        )
        # templatefile(x, y)
        main.add(lambda_layer)

        # Create Lambda function resources
        for lambda_task in self.lambda_functions:

            lambda_module = Module(
                lambda_task.name,
                source="terraform-aws-modules/lambda/aws",
                version="6.0.1",
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
                function_name=tf_format("%s-%s-%s", local.prefix, lambda_task.name, local.suffix),
                layers=[lambda_layer.ref("arn")],
                environment_variables={},
                create_role=True,
                attach_network_policy=True,
                attach_cloudwatch_logs_policy=True,
                attach_tracing_policy=True,
            )
            main.add(lambda_module)

        # Create Step Function State Machine resources
        for state_machine in self.state_machines:

            iam_assume_role_policy_document = Data(
                "aws_iam_policy_document",
                "role_assume_role_policy_{}".format(state_machine.name),
            )
            statement = ConfigBlock.nested(
                "statement",
                actions=["sts:AssumeRole"]
            )

            service_principal = ConfigBlock.nested(
                "principals",
                type="Service",
                identifiers=["states.amazonaws.com"]
            )
            statement.add_block(service_principal)
            iam_assume_role_policy_document.add_block(statement)

            iam_role_policy_document = Data(
                "aws_iam_policy_document",
                "role_role_policy_{}".format(state_machine.name),
            )
            statement_1 = ConfigBlock.nested(
                "statement",
                actions=["lambda:InvokeFunction"],
                resources=["*"]
            )
            statement_2 = ConfigBlock.nested(
                "statement",
                actions=["states:StartExecution"],
                resources=["*"]
            )
            iam_role_policy_document.add_block(statement_1)
            iam_role_policy_document.add_block(statement_2)

            data.add(iam_assume_role_policy_document)
            data.add(iam_role_policy_document)

            aws_iam_role = Resource(
                "aws_iam_role",
                "role_{}".format(state_machine.name),
                name=tf_format("%s-%s-%s", local.prefix, state_machine.name, local.suffix),
                assume_role_policy=iam_assume_role_policy_document.ref("json"),
                inline_policy_document=iam_role_policy_document.ref("json")
            )
            main.add(aws_iam_role)

            state_machine_resource = Resource(
                "aws_sfn_state_machine",
                state_machine.name,
                name=tf_format("%s-%s-%s", local.prefix, state_machine.name, local.suffix),
                role_arn=aws_iam_role.ref("role_arn"),
                definition="example-definition",
            )
            main.add(state_machine_resource)

        locals_block = Locals(
            prefix=AirFunctionsConfig().resource_prefix,
            suffix=AirFunctionsConfig().resource_suffix,
            environment=AirFunctionsConfig().environment,
            tags={}
        )
        locals.add(locals_block)

        locals.save("terraform/locals.tf")
        main.save("terraform/main.tf")
        data.save("terraform/data.tf")


if __name__ == "__main__":
    con1 = (step_1.output("a") == 10) | (step_1.output("b") == 20)
    branch_1 = (
        step_1 >> Pass("pass1") >> Choice(
            "Choice#1", default=step_2).choose(con1, step_3)
    )
    branch_1 = branch_1["Choice#1"].choice() >> Pass("next")
    branch_2 = step_4 >> [
        step_5,
        step_6 >> step_7,
    ] >> Pass(
        "pass2",
        input_path="$[0]",
        result={"output.$": "$.a"}
    )
    branch = branch_1 >> branch_2
    branch.to_statemachine("example-1")

    print(LambdaTaskContext._context)
    print(StateMachineContext._context)
    bundler = Bundler()
    bundler.collect_resources()
    bundler.to_terraform()
