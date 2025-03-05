import sys
from collections import deque
from dataclasses import dataclass
from typing import Any, Generic, Optional, Type, TypeVar

ModuleType = type(sys)
T = TypeVar('T')  # Type variable for the context objects


class ContextManagerMeta(type):
    """
    Metaclass for creating context manager classes for different object types.
    This allows the creation of context management capabilities for any class, not just StateMachine.
    """
    def __new__(mcs, name, bases, attrs):
        # Create a new class with the provided attributes
        cls = super().__new__(mcs, name, bases, attrs)

        # Initialize class variables for context management
        cls._context = deque()
        cls.autoregistered = set()
        cls.curr_registered_module_name = None

        return cls


class ContextManager(Generic[T], metaclass=ContextManagerMeta):
    """
    Generic context manager for any type of objects.
    Allows pushing, popping, and getting context objects of the specified type.
    """
    _context: deque[T] = deque()
    autoregistered: set[tuple[T, ModuleType]] = set()
    curr_registered_module_name: Optional[str] = None

    @classmethod
    def push_context_obj(cls, obj: T) -> None:
        """
        Push an object onto the context stack.

        Args:
            obj: The object to push onto the context stack
        """
        cls._context.appendleft(obj)

    @classmethod
    def pop_context_obj(cls) -> Optional[T]:
        """
        Pop an object from the context stack.
        If curr_registered_module_name is set, register the object with the module.

        Returns:
            The popped object, or None if the stack is empty
        """
        if not cls._context:
            return None

        obj = cls._context.popleft()

        if cls.curr_registered_module_name is not None and obj:
            mod = sys.modules[cls.curr_registered_module_name]
            cls.autoregistered.add((obj, mod))

        return obj

    @classmethod
    def get_curr_obj(cls) -> Optional[T]:
        """
        Get the current context object without removing it from the stack.

        Returns:
            The current context object, or None if the stack is empty
        """
        try:
            return cls._context[0]
        except IndexError:
            return None
