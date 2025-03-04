import operator
import sys
from dataclasses import dataclass
from typing import Any


ModuleType = type(sys)


def get_nested_value(data: dict, path: str, default: Any | None = None, delimiter: str = ".") -> Any:
    keys = path.split(delimiter)
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return default
    return data


@dataclass(frozen=True)
class Ref:
    attr_name: str

    def __eq__(self, o: object) -> object:
        return Condition(operator.eq, a=self.attr_name, b=o)

    def __ne__(self, o: object) -> object:
        return Condition(operator.ne, a=self.attr_name, b=o)

    def is_(self, o: object) -> object:
        return Condition(operator.is_, a=self.attr_name, b=o)

    def is_not(self, o: object) -> object:
        return Condition(operator.is_not, a=self.attr_name, b=o)

    def is_in(self, o: object) -> object:
        return Condition(operator.contains, a=self.attr_name, b=self)

    def __and__(self, o: object) -> object:
        return Condition(operator.and_, a=self.attr_name, b=o)

    def __or__(self, o: object) -> object:
        return Condition(operator.or_, a=self.attr_name, b=o)

    def __lt__(self, o: object) -> object:
        return Condition(operator.lt, a=self.attr_name, b=o)

    def __le__(self, o: object) -> object:
        return Condition(operator.le, a=self.attr_name, b=o)

    def __ge__(self, o: object) -> object:
        return Condition(operator.ge, a=self.attr_name, b=o)

    def __gt__(self, o: object) -> object:
        return Condition(operator.gt, a=self.attr_name, b=o)

    def __invert__(self) -> object:
        return Condition(operator.not_, a=self.attr_name)


@dataclass(frozen=True)
class Condition:
    operator: Any
    a: Any
    b: Any = None

    def __repr__(self) -> str:
        if self.b is None:
            return f'Condition({self.operator.__name__, self.a})'
        return f'Condition({self.operator.__name__, self.a, self.b})'

    def __eq__(self, o: object) -> object:
        return Condition(operator.eq, a=self, b=o)

    def __ne__(self, o: object) -> object:
        return Condition(operator.ne, a=self, b=o)

    def is_(self, o: object) -> object:
        return Condition(operator.is_, a=self, b=o)

    def is_not(self, o: object) -> object:
        return Condition(operator.is_not, a=self, b=o)

    def is_in(self, o: object) -> object:
        return Condition(operator.contains, a=o, b=self)

    def __and__(self, o: object) -> object:
        return Condition(operator.and_, a=self, b=o)

    def __or__(self, o: object) -> object:
        return Condition(operator.or_, a=self, b=o)

    def __lt__(self, o: object) -> object:
        return Condition(operator.lt, a=self, b=o)

    def __le__(self, o: object) -> object:
        return Condition(operator.le, a=self, b=o)

    def __ge__(self, o: object) -> object:
        return Condition(operator.ge, a=self, b=o)

    def __gt__(self, o: object) -> object:
        return Condition(operator.gt, a=self, b=o)

    def __invert__(self) -> object:
        return Condition(operator.not_, a=self)

    @staticmethod
    def parse_op(op):
        if op.__name__ == "eq":
            return "="
        elif op.__name__ in ("and_", "or_"):
            return op.__name__[:-1]
        elif op.__name__ == "gt":
            return ">"
        elif op.__name__ == "lt":
            return "<"
        elif op.__name__ == "ge":
            return ">="
        elif op.__name__ == "le":
            return "<="
        else:
            raise ValueError(f"Operator {op} not recognized.")

    @staticmethod
    def parse_arg(arg: Any) -> str:
        if isinstance(arg, Condition):
            return arg.jsonata(top=False)
        elif isinstance(arg, str) and not arg.startswith("$"):
            return '"' + str(arg) + '"'
        else:
            return str(arg)

    def jsonata(self, top=False) -> str:
        if self.b is None:
            res = f"{self.operator.__name__}({self.parse_arg(self.a)})"
        else:
            res = f"{self.parse_arg(self.a)} {self.parse_op(self.operator)} {self.parse_arg(self.b)}"
        if top:
            return f"{{% {res} %}}"
        return res

    def evaluate(self, event: dict, context: Any) -> bool:
        if self.b is None:
            a = self._eval_val(self.a, event)
            return self.operator(a)
        else:
            a = self._eval_val(self.a, event)
            b = self._eval_val(self.b, event)
            return self.operator(a, b)

    @staticmethod
    def _eval_val(val, event: dict) -> Any:
        if isinstance(val, Condition):
            return val.evaluate(event)
        elif isinstance(val, Ref):
            return get_nested_value(event, val.attr_name)
        elif isinstance(val, str) and val.startswith("$"):
            return get_nested_value(event, val.replace("$", ""))
        else:
            return val
