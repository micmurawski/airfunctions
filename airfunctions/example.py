from airfunctions.bundle import Config, TerraformBundler
from airfunctions.steps import Choice, Pass, lambda_task


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

    Config().resource_prefix = "project-name-"
    bundler = TerraformBundler()
    bundler.validate()
    bundler.apply()
