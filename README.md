# AirFunctions

`AirFunctions` is a Python framework that brings Airflow-like workflow orchestration to AWS Step Functions. It provides an intuitive Python API for building, testing, and deploying complex state machines while maintaining the reliability and scalability of AWS Step Functions.

## Features

- **Airflow-like Python API**: Write AWS Step Functions workflows using familiar Python syntax
- **Local Testing**: Test your workflows locally before deployment
- **Easy Composition**: Build complex state machines using simple operators like `>>` and conditional logic
- **Fast Deployment**: Streamlined deployment process using Terraform
- **Lambda Integration**: Seamless integration with AWS Lambda functions

## Quick Start

```python
from airfunctions.bundle import Config, TerraformBundler
from airfunctions.steps import Choice, Pass, lambda_task

# Define your Lambda tasks
@lambda_task
def step_1(event, context):
    return event

# Create workflows using familiar operators
workflow = (
    step_1 
    >> Pass("pass1") 
    >> Choice("my_choice", default=step_2).choose(
        condition=step_1.output("value") == 10, 
        next_step=step_3
    )
)

# Deploy to AWS Step Functions
workflow.to_statemachine("my-workflow")

# Configure and deploy using Terraform
Config().resource_prefix = "my-project-"
bundler = TerraformBundler()
bundler.validate()
bundler.apply()
```

## Key Concepts

- **Lambda Tasks**: Decorate your Python functions with `@lambda_task` to convert them into AWS Lambda functions
- **Flow Operators**: Use `>>` to chain steps together
- **Conditional Logic**: Build branching workflows using `Choice` steps
- **Parallel Execution**: Run steps in parallel using list syntax
- **State Management**: Access task outputs using the `.output()` method

## Installation

```bash
pip install airfunctions
```
