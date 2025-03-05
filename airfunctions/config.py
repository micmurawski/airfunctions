import sys
from collections import deque
from dataclasses import dataclass

ModuleType = type(sys)


class StateMachine:
    pass


class StateMachineContext:
    _context: deque[StateMachine] = deque()
    autoregistered: set[tuple[StateMachine, ModuleType]] = set()
    curr_registered_module_name: str | None = None

    @classmethod
    def push_context_obj(cls, obj: StateMachine):
        cls._context.appendleft(obj)

    @classmethod
    def pop_context_managed_sm(cls) -> StateMachine | None:
        obj = cls._context.popleft()

        if cls.curr_registered_module_name is not None and obj:  # and obj.auto_register:
            mod = sys.modules[cls.curr_registered_module_name]
            cls.autoregistered.add((obj, mod))

        return obj

    @classmethod
    def get_curr_obj(cls) -> StateMachine | None:
        try:
            return cls._context[0]
        except IndexError:
            return None


class ObjectContext:
    _context = deque()


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
