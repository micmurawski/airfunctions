from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable

from airfunctions.conditions import Condition, Ref
from airfunctions.context import ContextManager
from airfunctions.jsonpath import JSONPath


class AWSResource(str, Enum):
    AWS_LAMBDA = "arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:function:${prefix}${FUNCTION_NAME}${suffix}"
    AWS_STATES_LAMBDA_INVOKE = "arn:aws:states:::lambda:invoke"
    AWS_STATES_STATE_MACHINE = "arn:aws:states:${AWS_REGION}:${AWS_ACCOUNT_ID}:stateMachine:${prefix}${STATE_MACHINE}${suffix}"
    AWS_STATES_START_EXECUTION = "arn:aws:states:::states:startExecution"
    AWS_STATES_START_EXECUTION_SYNC = "arn:aws:states:::states:startExecution.sync:2"


def collect_steps(step: str, content: dict, q: deque):
    if step not in content["States"]:
        return

    if content["States"][step]["Type"] == "Choice":
        for choice in content["States"][step]["Choices"]:
            q.appendleft(choice["Next"])
        if "Default" in content["States"][step]:
            q.appendleft(content["States"][step]["Default"])
    else:
        q.appendleft(step)


def find_ends(step: str, content: dict) -> list[str]:
    queue = deque([])
    collect_steps(step, content, queue)
    ends = set()

    while queue:
        nxt = queue.popleft()

        while content["States"][nxt]['Type'] != "Choice" and not content["States"][nxt].get("End", False):
            nxt = content["States"][nxt]["Next"]
        if content["States"][nxt]['Type'] == "Choice":
            collect_steps(nxt, content, queue)
            continue

        ends.add(nxt)
    return list(ends)


@dataclass(frozen=True, init=True)
class Branch:
    def __init__(self, head: Any, steps: dict | None = None, branch: Any | None = None):
        object.__setattr__(self, "head", head)
        object.__setattr__(self, "branch", branch)

        if steps:
            object.__setattr__(self, "steps", steps)
            for _, step in self.steps.items():
                self.add_step(step)
        else:
            object.__setattr__(self, "steps", {})

        self.steps[head.name] = head
        self.steps[head.name].branch = self

    def add_step(self, step):
        self.steps[step.name] = step
        step.branch = self

    def __getitem__(self, key: str):
        return self.steps[key]

    def add_end(self, step):
        if step:
            ends = find_ends(self.head.name, self.definition)
            for end in ends:
                self.add_step(step)
                self.steps[end].set_next(step)

    @classmethod
    def merge_branches(cls, a, b, merge_at: list[str] | None = None):
        _a = deepcopy(a)
        _b = deepcopy(b)
        _head = _a.head

        if merge_at is None:
            merge_at = (None, _b.head.name)

        _steps = {**_a.steps, **_b.steps}
        new_branch = Branch(_head, _steps)

        if merge_at[0]:
            new_branch.steps[merge_at].set_next(merge_at[1])
        else:
            new_branch.add_end(new_branch.steps[merge_at[1]])

        return new_branch

    def __rshift__(self, nxt: Any):
        if isinstance(nxt, Branch):
            return self.merge_branches(self, nxt)

        _steps = deepcopy(self.steps)
        _nxt = deepcopy(nxt)
        _head = _steps[self.head.name]

        if isinstance(_nxt, Choice):
            choices = deepcopy(list(_nxt.branch.steps.values()))
            for c in choices:
                _steps[c.name] = c
        else:
            _steps[_nxt.name] = _nxt

        new_branch = Branch(_head, _steps)
        new_branch.add_end(_nxt)
        return new_branch

    def __repr__(self):
        steps_names = list(self.steps.keys())
        return f"Branch({', '.join(steps_names)})"

    @property
    def name(self):
        return "-".join(list(self.steps.keys()))

    @property
    def definition(self) -> dict:
        return {
            "StartAt": self.head.name,
            "States": dict((k, v._content) for k, v in self.steps.items()),
        }

    def to_statemachine(self, name: str) -> Any:
        return StateMachine(name, branch=self)

    @staticmethod
    def __call_choice(curr, event, context):
        con: Condition
        for con, step_name in curr.choices.items():
            if con.evaluate(event, context):
                return curr.branch.steps[step_name]
        return curr.branch.steps[curr.default]

    def __call__(self, event: dict, context: Any):
        curr: Step = self.head
        _in = event
        _context = context
        while True:
            _in = curr._parse_input(_in)
            if isinstance(curr, Choice):
                curr = self.__call_choice(curr, _in, context)
                continue
            _out = curr(_in, _context)
            _in = curr._parse_output(_out)
            if curr.end:
                break
            curr = self.steps[curr.next]
        return _in


class Step:
    def __init__(
            self,
            name: str,
            type: str,
            query_language: str | None = None,
            input_path: str | None = None,
            result_path: str | None = None,
            output_path: str | None = None,
            comment: str | None = None,
            branch: Branch | None = None,
    ):
        self.name = name
        self._content = {"Type": type, "End": True}

        if query_language:
            self._content["QueryLanguage"] = query_language

        if input_path:
            self._content["InputPath"] = input_path

        if result_path:
            self._content["ResultPath"] = result_path

        if output_path:
            self._content["OutputPath"] = output_path

        if comment:
            self._content["Comment"] = comment

        self.branch = branch
        self.catchers: dict[tuple[str], Step] = {}

    @property
    def input_path(self):
        return self._content.get("InputPath")

    @property
    def result_path(self):
        return self._content.get("ResultPath")

    @property
    def output_path(self):
        return self._content.get("OutputPath")

    def _parse_input(self, input_data) -> Any:
        jsonpath = JSONPath()
        if getattr(self, "input_path", None):
            effective_input = jsonpath.apply(self.input_path, input_data)
        else:
            effective_input = input_data

        if getattr(self, "parameters", None):
            return jsonpath.process_payload_template(self.parameters, effective_input)
        return effective_input

    def _parse_output(self, output_data) -> Any:
        jsonpath = JSONPath()
        if getattr(self, "result_path", None):
            effective_output = jsonpath.apply(self.result_path, output_data)
        else:
            effective_output = output_data

        if getattr(self, "output_path", None):
            return jsonpath.apply(self.output_path, effective_output)
        return effective_output

    @property
    def end(self) -> bool:
        return self.next is None

    @property
    def next(self) -> str | None:
        return self._content.get("Next")

    def set_branch(self, branch: Branch):
        self.branch = branch

    def set_next(self, nxt: Any):
        if nxt:
            self._content["Next"] = nxt.name
            self._content.pop("End", None)

    def __rshift__(self, next: Any) -> Branch:
        if isinstance(next, Branch):
            if self.branch:
                new_branch = Branch.merge_branches(
                    self.branch,
                    next,
                    merge_at=(self.name, next.head.name)
                )
                return new_branch
            else:
                new_branch = deepcopy(next)
                new_branch.add_end(self)
                return new_branch

        _next = deepcopy(next)
        if isinstance(_next, Step):
            if next != self and self.branch is None:
                branch = Branch(head=self)
                branch.add_end(_next)
                return branch
            elif self.branch:
                _next.branch = self.branch
                self.branch.steps[_next.name] = _next
                self.branch.steps[self.name].set_next(_next)
                return self.branch
            else:
                raise ValueError("step cannot be attached to itself.")

        if isinstance(_next, (list, tuple)):
            pnext: Parallel = parallel(*_next)

            if self.branch:
                branch = self.branch
            else:
                branch = Branch(head=self)

            branch.add_end(pnext)
            return branch
        return _next

    def retry(
        self,
        error_equals: list[str, Exception] | Exception | str,
        interval_seconds: int,
        max_attempts: int,
        max_delay_seconds: int | None = None,
        back_off_rate: float | None = None
    ):
        if "Retry" not in self._content:
            self._content["Retry"] = []

        retry = {}
        if isinstance(error_equals, (str, Exception)):
            retry["ErrorEquals"] = [str(error_equals), ]
        else:
            retry["ErrorEquals"] = [str(err) for err in error_equals]

        if interval_seconds:
            retry["IntervalSeconds"] = interval_seconds
        if max_attempts:
            retry["MaxAttempts"] = max_attempts
        if max_delay_seconds:
            retry["MaxDelaySeconds"] = max_delay_seconds
        if back_off_rate:
            retry["BackOffRate"] = back_off_rate

        self._content["Retry"].append(retry)

    def catch(self, error_equals: list[str, Exception] | Exception | str, nxt: Any):
        if "Catch" not in self._content:
            self._content["Catch"] = []

        _catch = {}
        if isinstance(error_equals, (str, Exception)):
            _catch["ErrorEquals"] = (str(error_equals), )
        else:
            _catch["ErrorEquals"] = tuple([str(err) for err in error_equals])

        _key = _catch["ErrorEquals"]
        self.catchers[_key] = deepcopy(nxt)
        _next = self.catchers[_key]

        try:
            idx, _ = next(filter(
                lambda _, item: item["ErrorEquals"] == _key, enumerate(self._content["Catch"])))
            self._content["Catch"][idx]["Next"] = _next.name
        except StopIteration:
            _catch["Next"] = _next.name
            self._content["Catch"].append(_catch)
        return self

    def catcher(self, error_equals: list[str, Exception] | Exception | str) -> Any:
        if isinstance(error_equals, (str, Exception)):
            _key = (str(error_equals), )
        else:
            _key = tuple([str(err) for err in error_equals])

        if not self.branch:
            return self.catchers[_key]
        _name = self.catchers[_key].name
        return self.branch.steps[_name]

    def __repr__(self):
        return f"{self._content['Type']}(name={self.name})"


class Task(Step):
    def __init__(self,
                 name: str,
                 resource: str,
                 parameters: dict | None = None,
                 query_language: str | None = None,
                 input_path: str | None = None,
                 result_path: str | None = None, output_path: str | None = None,
                 comment: str | None = None, **kwargs):
        super().__init__(name, "Task", query_language, input_path,
                         result_path, output_path, comment, **kwargs)

        self.parameters = parameters
        self.resource = resource
        self._content["Resource"] = self.resource

        if self.parameters:
            self._content["Parameters"] = self.parameters


class Choice(Step):
    end = False

    def __init__(self,
                 name: str,
                 query_language: str | None = None,
                 input_path: str | None = None,
                 default: Step | None = None,
                 comment: str | None = None, **kwargs):
        super().__init__(name, "Choice", query_language,
                         input_path, None, None, comment, **kwargs)
        self._content.pop("End", None)

        self.choices: dict[Condition, str] = {}
        self._content["Choices"] = []

        self.branch = Branch(head=self)
        self.branch.add_step(default)
        self.default = default.name
        if self.default:
            self._content["Default"] = self.default

    def choice(self, condition: Condition | None = None) -> Step | Branch | None:
        if condition:
            _name = self.choices[condition]
            return self.branch.steps[_name]
        else:
            if condition:
                _name = self.choices[condition].name
                return self.branch.steps[_name]
            else:
                return self.branch.steps[self.default]

    def choose(self, condition: Condition, next: Step):
        _next = deepcopy(next)
        self.branch.add_step(_next)

        self.choices[condition] = _next.name
        try:
            condition_idx = [
                item["Condition"] for item in self._content["Choices"]
            ].index(condition)
            self._content["Choices"][condition_idx]["Next"] = _next.name
        except ValueError:
            self._content["Choices"].append(
                {"Condition": condition.jsonata(), "Next": _next.name}
            )
        return self

    def __repr__(self):
        choices = []
        for con, step_name in self.choices.items():
            step = self.branch[step_name]
            choices.append(f"{con}>>{step}")
        if self.default:
            choices.append(f"default>>{self.default}")

        return (
            f"{self._content["Type"]}(name={self.name}, choices={', '.join(choices)})"
        )


class Parallel(Step):
    def __init__(
        self,
            name,
            branches: list[Step | Branch] | None = None,
            query_language=None,
            input_path=None,
            result_path=None,
            output_path=None,
            comment=None,
            branch=None,
            **kwargs,
    ):
        super().__init__(name, "Parallel", query_language, input_path,
                         result_path, output_path, comment, branch, **kwargs)
        if branches:
            self.branches = branches
        else:
            self.branches = []

        self._content["Branches"] = []
        branch: Step | Branch
        for branch in self.branches:
            if isinstance(branch, Step):
                self._content["Branches"].append(
                    Branch(head=branch).definition
                )
            if isinstance(branch, Branch):
                self._content["Branches"].append(
                    branch.definition)

    def __call__(self, event, context, *args, **kwds):
        return [_branch(event, context) for _branch in self.branches]


def parallel(*branches: list[Step | Branch], **kwargs) -> Parallel:
    names = []
    for step in branches:
        names.append(step.name)
    name = '|'.join(names)
    return Parallel(name=name, branches=branches, **kwargs)


class Wait(Step):
    def __init__(
            self,
            name: str,
            seconds: Ref | str | float | int | None = None,
            seconds_path: Ref | str | None = None,
            timestamp_path: Ref | str | None = None,
            comment: str | None = None,
            branch: Branch | None = None,
            **kwargs
    ):
        super().__init__(name, type="Wait", comment=comment, branch=branch, **kwargs)
        if not seconds and not seconds_path and not timestamp_path:
            raise ValueError(
                "seconds, seconds_path, and timestamp_path are None. At least one needs to be defined.")

        if seconds:
            self._content["Seconds"] = seconds

        if seconds_path:
            self._content["SecondsPath"] = str(seconds_path)

        if timestamp_path:
            self._content["TimestampPath"] = str(timestamp_path)


class Pass(Step):
    def __init__(
        self,
        name: str,
        query_language: str | None = None,
        input_path: str | None = None,
        result_path: str | None = None,
        output_path: str | None = None,
        result: Any | None = None,
        output: Any | None = None,
        comment: str | None = None,
        branch: str | None = None,
        **kwargs
    ):
        super().__init__(
            name, "Pass", query_language, input_path,
            result_path, output_path, comment, branch, **kwargs)

        if result:
            self._content["Result"] = result

        if output:
            self._content["Output"] = output

    def __call__(self, event: dict, context: Any, *args, **kwargs):
        if "Result" in self._content:
            return JSONPath().process_payload_template(self._content["Result"], event, context)
        return event


class Succeed(Step):
    end = True

    def __init__(self, name: str, output: Ref | str | None = None):
        super().__init__(name, "Succeed", None, None)
        if output:
            self._content["Output"] = output


class Fail(Step):
    end = True

    def __init__(
            self,
            name: str,
            cause: Ref | str | None = None,
            cause_path: Ref | str | None = None,
            error_path: Ref | str | None = None
    ):
        super().__init__(name, "Fail", None, None)
        if cause:
            self._content["Cause"] = str(cause)
        if cause_path:
            self._content["CausePath"] = str(cause_path)
        if error_path:
            self._content["ErrorPath"] = str(error_path)


class StateMachine(Task):
    def __init__(
        self,
        name: str,
        branch: Any,
        parameters=None,
        query_language=None,
        input_path=None,
        result_path=None,
        output_path=None,
        comment=None,
        **kwargs
    ):
        self.arn = AWSResource.AWS_STATES_STATE_MACHINE.value.replace(
            "${STATE_MACHINE}", name)
        self.sm_branch = branch

        if parameters is None:
            parameters = {}

        parameters["StateMachineArn"] = self.arn

        super().__init__(
            name,
            AWSResource.AWS_STATES_START_EXECUTION_SYNC.value,
            parameters,
            query_language,
            input_path,
            result_path,
            output_path,
            comment,
            **kwargs
        )
        StateMachineContext.push_context_obj(self)

    def __repr__(self):
        return f"StateMachine(name={self.name})"

    @property
    def definition(self) -> dict:
        return self.sm_branch.definition


class StateMachineContext(ContextManager[StateMachine]):
    """Context manager specifically for StateMachine objects."""
    pass


class LambdaFunction(Task):
    def __init__(self, func: Callable | None = None,
                 *,
                 module_path: str | None = None,
                 parameters: dict | None = None,
                 query_language: str | None = None,
                 input_path: str | None = None,
                 result_path: str | None = None, output_path: str | None = None,
                 comment: str | None = None,
                 timeout: int = 900,
                 memory_size: int = 256,
                 tracing_mode: str = "Active",
                 **kwargs):
        self.func = func
        self.timeout = timeout
        self.memory_size = memory_size
        self.tracing_mode = tracing_mode
        self.handler_path = (".".join(self.func.__module__.split(".")) + "." + self.func.__name__) or (module_path + "." + self.func.__name__)

        resource = AWSResource.AWS_LAMBDA.value.replace(
            "${FUNCTION_NAME}", self.func.__name__)

        super().__init__(
            self.func.__name__,
            resource,
            parameters,
            query_language,
            input_path,
            result_path,
            output_path,
            comment,
            **kwargs
        )
        self.func = func
        self.__qualname__ = self.func.__qualname__
        self.__name__ = self.func.__name__
        self.__doc__ = self.func.__doc__

    def __repr__(self):
        return f"{self.__class__.__name__}({repr(self.func)})"

    def output(self, path: str):
        return Ref(path)

    def __call__(self, event, context, *args, **kwargs):
        return self.func(event, context, *args, **kwargs)


class LambdaTaskContext(ContextManager[LambdaFunction]):
    """Context manager specifically for LambdaFunction objects."""
    pass


def lambda_task(func: Callable, **kwargs) -> LambdaFunction:
    _lambda_function = LambdaFunction(func, **kwargs)
    LambdaTaskContext.push_context_obj(_lambda_function)
    return _lambda_function


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
