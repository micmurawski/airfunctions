from dataclasses import dataclass


def singleton(class_):
    instances = {}

    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance


@singleton
@dataclass
class AirFunctionsConfig:
    aws_region: str
    aws_account_id: str
    resource_prefix: str = ""
    resource_suffix: str = ""


if __name__ == "__main__":
    config = AirFunctionsConfig()
    import os
    print(config == AirFunctionsConfig(
        aws_region=os.environ.get("AWS_REGION"),
        aws_account_id=os.environ.get("AWS_ACCOUNT_ID")
    ))
