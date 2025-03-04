import operator
import sys
import re
import json
import random
import uuid
import base64
import hashlib
from dataclasses import dataclass
from typing import Any, List, Dict, Union, Callable, Optional, Tuple, Set


ModuleType = type(sys)


# JSONPath specific implementation starts here
class JSONPathError(Exception):
    """Base exception for JSONPath errors."""
    pass


class JSONPathToken:
    """Represents a token in a JSONPath expression."""
    ROOT = 'ROOT'          # $
    CURRENT = 'CURRENT'    # @
    DOT = 'DOT'            # .
    RECURSIVE = 'RECURSIVE'  # ..
    WILDCARD = 'WILDCARD'  # *
    NAME = 'NAME'          # identifier
    LBRACKET = 'LBRACKET'  # [
    RBRACKET = 'RBRACKET'  # ]
    LPAREN = 'LPAREN'      # (
    RPAREN = 'RPAREN'      # )
    COMMA = 'COMMA'        # ,
    COLON = 'COLON'        # :
    NUMBER = 'NUMBER'      # 123
    STRING = 'STRING'      # 'abc'
    FILTER = 'FILTER'      # ?

    def __init__(self, type: str, value: Any = None):
        self.type = type
        self.value = value

    def __repr__(self):
        if self.value is not None:
            return f"<{self.type}:{self.value}>"
        return f"<{self.type}>"


class JSONPathLexer:
    """Tokenizes a JSONPath expression."""

    def __init__(self, path: str):
        self.path = path
        self.pos = 0
        self.current_char = self.path[0] if path else None

    def error(self, message: str):
        raise JSONPathError(f"Lexer error at position {self.pos}: {message}")

    def advance(self):
        """Move the pointer to the next character."""
        self.pos += 1
        if self.pos >= len(self.path):
            self.current_char = None
        else:
            self.current_char = self.path[self.pos]

    def peek(self, n: int = 1) -> Optional[str]:
        """Look ahead n characters without advancing."""
        peek_pos = self.pos + n
        if peek_pos >= len(self.path):
            return None
        return self.path[peek_pos]

    def skip_whitespace(self):
        """Skip whitespace characters."""
        while self.current_char is not None and self.current_char.isspace():
            self.advance()

    def number(self) -> JSONPathToken:
        """Parse a number token."""
        result = ''
        while self.current_char is not None and (self.current_char.isdigit() or self.current_char == '-'):
            result += self.current_char
            self.advance()

        if result.startswith('-'):
            return JSONPathToken(JSONPathToken.NUMBER, int(result))
        return JSONPathToken(JSONPathToken.NUMBER, int(result))

    def string(self) -> JSONPathToken:
        """Parse a string token enclosed in quotes."""
        quote_char = self.current_char  # Either ' or "
        self.advance()  # Skip the opening quote

        result = ''
        while self.current_char is not None and self.current_char != quote_char:
            if self.current_char == '\\' and self.peek() == quote_char:
                self.advance()  # Skip the escape character
                result += quote_char
            else:
                result += self.current_char
            self.advance()

        if self.current_char is None:
            self.error(
                f"Unterminated string starting at position {self.pos - len(result) - 1}")

        self.advance()  # Skip the closing quote
        return JSONPathToken(JSONPathToken.STRING, result)

    def name(self) -> JSONPathToken:
        """Parse an identifier/name token."""
        result = ''
        while (self.current_char is not None and
               (self.current_char.isalnum() or self.current_char == '_')):
            result += self.current_char
            self.advance()

        return JSONPathToken(JSONPathToken.NAME, result)

    def get_next_token(self) -> Optional[JSONPathToken]:
        """Get the next token from the input."""
        while self.current_char is not None:

            # Skip whitespace
            if self.current_char.isspace():
                self.skip_whitespace()
                continue

            # Root symbol
            if self.current_char == '$':
                self.advance()
                return JSONPathToken(JSONPathToken.ROOT)

            # Current node symbol
            if self.current_char == '@':
                self.advance()
                return JSONPathToken(JSONPathToken.CURRENT)

            # Dot notation
            if self.current_char == '.':
                self.advance()
                # Check for recursive descent (..)
                if self.current_char == '.':
                    self.advance()
                    return JSONPathToken(JSONPathToken.RECURSIVE)
                return JSONPathToken(JSONPathToken.DOT)

            # Wildcard
            if self.current_char == '*':
                self.advance()
                return JSONPathToken(JSONPathToken.WILDCARD)

            # Brackets, parentheses, and other punctuation
            if self.current_char == '[':
                self.advance()
                return JSONPathToken(JSONPathToken.LBRACKET)

            if self.current_char == ']':
                self.advance()
                return JSONPathToken(JSONPathToken.RBRACKET)

            if self.current_char == '(':
                self.advance()
                return JSONPathToken(JSONPathToken.LPAREN)

            if self.current_char == ')':
                self.advance()
                return JSONPathToken(JSONPathToken.RPAREN)

            if self.current_char == ',':
                self.advance()
                return JSONPathToken(JSONPathToken.COMMA)

            if self.current_char == ':':
                self.advance()
                return JSONPathToken(JSONPathToken.COLON)

            if self.current_char == '?':
                self.advance()
                return JSONPathToken(JSONPathToken.FILTER)

            # Numbers
            if self.current_char.isdigit() or self.current_char == '-':
                return self.number()

            # Strings
            if self.current_char in ("'", '"'):
                return self.string()

            # Names/identifiers
            if self.current_char.isalpha() or self.current_char == '_':
                return self.name()

            self.error(f"Unexpected character: {self.current_char}")

        # End of input
        return None


class JSONPathParser:
    """Parser for JSONPath expressions."""

    def __init__(self, lexer: JSONPathLexer):
        self.lexer = lexer
        self.current_token = self.lexer.get_next_token()

    def error(self, message: str):
        raise JSONPathError(f"Parser error: {message}")

    def eat(self, token_type: str):
        """Consume the current token if it matches the expected type."""
        if self.current_token and self.current_token.type == token_type:
            self.current_token = self.lexer.get_next_token()
        else:
            self.error(f"Expected {token_type}, got {self.current_token}")

    def parse(self) -> List[Dict[str, Any]]:
        """Parse the JSONPath expression into a list of operations."""
        operations = []

        # Expect the path to start with $
        if not self.current_token or self.current_token.type != JSONPathToken.ROOT:
            self.error("JSONPath must start with '$'")
        self.eat(JSONPathToken.ROOT)

        # Parse the rest of the path
        while self.current_token:
            if self.current_token.type == JSONPathToken.DOT:
                self.eat(JSONPathToken.DOT)

                # Handle .name
                if self.current_token and self.current_token.type == JSONPathToken.NAME:
                    name = self.current_token.value
                    self.eat(JSONPathToken.NAME)
                    operations.append({'op': 'field', 'name': name})

                # Handle .* (wildcard)
                elif self.current_token and self.current_token.type == JSONPathToken.WILDCARD:
                    self.eat(JSONPathToken.WILDCARD)
                    operations.append({'op': 'wildcard'})

                else:
                    self.error("Expected name or wildcard after dot")

            elif self.current_token.type == JSONPathToken.RECURSIVE:
                self.eat(JSONPathToken.RECURSIVE)

                # Handle ..name
                if self.current_token and self.current_token.type == JSONPathToken.NAME:
                    name = self.current_token.value
                    self.eat(JSONPathToken.NAME)
                    operations.append(
                        {'op': 'recursive_descent', 'name': name})

                # Handle ..* (recursive wildcard)
                elif self.current_token and self.current_token.type == JSONPathToken.WILDCARD:
                    self.eat(JSONPathToken.WILDCARD)
                    operations.append({'op': 'recursive_wildcard'})

                else:
                    self.error(
                        "Expected name or wildcard after recursive descent")

            elif self.current_token.type == JSONPathToken.LBRACKET:
                self.eat(JSONPathToken.LBRACKET)

                # Handle [*] (wildcard)
                if self.current_token and self.current_token.type == JSONPathToken.WILDCARD:
                    self.eat(JSONPathToken.WILDCARD)
                    self.eat(JSONPathToken.RBRACKET)
                    operations.append({'op': 'wildcard'})

                # Handle [number] (array index)
                elif self.current_token and self.current_token.type == JSONPathToken.NUMBER:
                    index = self.current_token.value
                    self.eat(JSONPathToken.NUMBER)

                    # Check if this is a slice [start:end:step]
                    if self.current_token and self.current_token.type == JSONPathToken.COLON:
                        self.eat(JSONPathToken.COLON)

                        # Get end index if specified
                        end = None
                        if self.current_token and self.current_token.type == JSONPathToken.NUMBER:
                            end = self.current_token.value
                            self.eat(JSONPathToken.NUMBER)

                        # Check for step
                        step = 1
                        if self.current_token and self.current_token.type == JSONPathToken.COLON:
                            self.eat(JSONPathToken.COLON)
                            if self.current_token and self.current_token.type == JSONPathToken.NUMBER:
                                step = self.current_token.value
                                self.eat(JSONPathToken.NUMBER)

                        self.eat(JSONPathToken.RBRACKET)
                        operations.append(
                            {'op': 'slice', 'start': index, 'end': end, 'step': step})

                    # Check if this is part of a multi-index [1,2,3]
                    elif self.current_token and self.current_token.type == JSONPathToken.COMMA:
                        indices = [index]
                        while self.current_token and self.current_token.type == JSONPathToken.COMMA:
                            self.eat(JSONPathToken.COMMA)
                            if self.current_token and self.current_token.type == JSONPathToken.NUMBER:
                                indices.append(self.current_token.value)
                                self.eat(JSONPathToken.NUMBER)
                            else:
                                self.error(
                                    "Expected number after comma in multi-index")

                        self.eat(JSONPathToken.RBRACKET)
                        operations.append(
                            {'op': 'multi_index', 'indices': indices})

                    # Single index
                    else:
                        self.eat(JSONPathToken.RBRACKET)
                        operations.append({'op': 'index', 'index': index})

                # Handle ['name'] or ["name"] (quoted field name)
                elif self.current_token and self.current_token.type == JSONPathToken.STRING:
                    name = self.current_token.value
                    self.eat(JSONPathToken.STRING)

                    # Check if this is part of a multi-name ['name1','name2']
                    if self.current_token and self.current_token.type == JSONPathToken.COMMA:
                        names = [name]
                        while self.current_token and self.current_token.type == JSONPathToken.COMMA:
                            self.eat(JSONPathToken.COMMA)
                            if self.current_token and self.current_token.type == JSONPathToken.STRING:
                                names.append(self.current_token.value)
                                self.eat(JSONPathToken.STRING)
                            else:
                                self.error(
                                    "Expected string after comma in multi-name")

                        self.eat(JSONPathToken.RBRACKET)
                        operations.append(
                            {'op': 'multi_field', 'names': names})
                    else:
                        self.eat(JSONPathToken.RBRACKET)
                        operations.append({'op': 'field', 'name': name})

                # Handle filter expressions [?(...)]
                elif self.current_token and self.current_token.type == JSONPathToken.FILTER:
                    self.eat(JSONPathToken.FILTER)
                    # In a real implementation, we would parse filter expressions here
                    # For simplicity, we'll just capture the entire filter text
                    # This is a simplification; a real implementation would parse this properly
                    self.eat(JSONPathToken.LPAREN)
                    # TODO: Parse filter expression
                    filter_expr = "..."  # Placeholder
                    self.eat(JSONPathToken.RPAREN)
                    self.eat(JSONPathToken.RBRACKET)
                    operations.append({'op': 'filter', 'expr': filter_expr})

                else:
                    self.error("Unexpected token inside brackets")

            else:
                self.error(f"Unexpected token: {self.current_token}")

        return operations


class JSONPathEvaluator:
    """Evaluates JSONPath expressions against JSON data."""

    def __init__(self, operations: List[Dict[str, Any]]):
        self.operations = operations

    def evaluate(self, data: Any) -> List[Any]:
        """Evaluate the JSONPath operations against the input data."""
        result = [data]

        for op in self.operations:
            result = self._apply_operation(op, result)

        return result

    def _apply_operation(self, operation: Dict[str, Any], nodes: List[Any]) -> List[Any]:
        """Apply a single operation to a list of nodes."""
        op_type = operation['op']
        result = []

        if op_type == 'field':
            for node in nodes:
                if isinstance(node, dict) and operation['name'] in node:
                    result.append(node[operation['name']])

        elif op_type == 'wildcard':
            for node in nodes:
                if isinstance(node, dict):
                    result.extend(node.values())
                elif isinstance(node, list):
                    result.extend(node)

        elif op_type == 'index':
            for node in nodes:
                if isinstance(node, list) and 0 <= operation['index'] < len(node):
                    result.append(node[operation['index']])
                elif isinstance(node, list) and operation['index'] < 0 and abs(operation['index']) <= len(node):
                    # Handle negative indices (count from the end)
                    result.append(node[operation['index']])

        elif op_type == 'multi_index':
            for node in nodes:
                if isinstance(node, list):
                    for idx in operation['indices']:
                        if 0 <= idx < len(node):
                            result.append(node[idx])
                        elif idx < 0 and abs(idx) <= len(node):
                            result.append(node[idx])

        elif op_type == 'slice':
            start = operation['start']
            end = operation['end']
            step = operation['step']

            for node in nodes:
                if isinstance(node, list):
                    if end is None:
                        end = len(node)
                    result.extend(node[start:end:step])

        elif op_type == 'multi_field':
            for node in nodes:
                if isinstance(node, dict):
                    for name in operation['names']:
                        if name in node:
                            result.append(node[name])

        elif op_type == 'recursive_descent':
            # Implementation for recursive descent
            def collect_matching(current_node, field_name):
                matches = []

                if isinstance(current_node, dict):
                    # Check if the current node has the field
                    if field_name in current_node:
                        matches.append(current_node[field_name])

                    # Recursively check all values
                    for value in current_node.values():
                        matches.extend(collect_matching(value, field_name))

                elif isinstance(current_node, list):
                    # Recursively check all items
                    for item in current_node:
                        matches.extend(collect_matching(item, field_name))

                return matches

            for node in nodes:
                result.extend(collect_matching(node, operation['name']))

        elif op_type == 'recursive_wildcard':
            # Implementation for recursive wildcard
            def collect_all(current_node):
                matches = []

                if isinstance(current_node, dict):
                    # Add all values
                    matches.extend(current_node.values())

                    # Recursively collect from all values
                    for value in current_node.values():
                        matches.extend(collect_all(value))

                elif isinstance(current_node, list):
                    # Add all items
                    matches.extend(current_node)

                    # Recursively collect from all items
                    for item in current_node:
                        matches.extend(collect_all(item))

                return matches

            for node in nodes:
                result.extend(collect_all(node))

        elif op_type == 'filter':
            # Simplified filter implementation
            # In a real implementation, we would evaluate filter expressions
            # This is a placeholder
            pass

        return result


class JSONPathIntrinsicFunctions:
    """Implementation of JSONPath intrinsic functions."""

    @staticmethod
    def format(template: str, *args) -> str:
        """
        Implements States.Format function.
        Replaces {} placeholders with argument values.
        """
        # Replace escaped characters
        template = template.replace(
            '\\{', '{').replace('\\}', '}').replace('\\\\', '\\')

        # Count number of {} pairs
        placeholders = template.count('{}')
        if placeholders != len(args):
            raise JSONPathError(
                f"States.Format expected {placeholders} arguments, got {len(args)}")

        # Format the string
        return template.format(*args)

    @staticmethod
    def string_to_json(s: str) -> Any:
        """
        Implements States.StringToJson function.
        Parses a JSON string into a JSON object.
        """
        try:
            return json.loads(s)
        except json.JSONDecodeError as e:
            raise JSONPathError(f"States.StringToJson error: {str(e)}")

    @staticmethod
    def json_to_string(obj: Any) -> str:
        """
        Implements States.JsonToString function.
        Converts a JSON object to a JSON string.
        """
        try:
            return json.dumps(obj)
        except (TypeError, ValueError) as e:
            raise JSONPathError(f"States.JsonToString error: {str(e)}")

    @staticmethod
    def array(*args) -> List[Any]:
        """
        Implements States.Array function.
        Returns an array containing the arguments.
        """
        return list(args)

    @staticmethod
    def array_partition(arr: List[Any], chunk_size: int) -> List[List[Any]]:
        """
        Implements States.ArrayPartition function.
        Partitions an array into chunks of specified size.
        """
        if not isinstance(arr, list):
            raise JSONPathError(
                "States.ArrayPartition: First argument must be an array")

        if not isinstance(chunk_size, int) or chunk_size <= 0:
            raise JSONPathError(
                "States.ArrayPartition: Second argument must be a positive integer")

        result = []
        for i in range(0, len(arr), chunk_size):
            result.append(arr[i:i + chunk_size])

        return result

    @staticmethod
    def array_contains(arr: List[Any], value: Any) -> bool:
        """
        Implements States.ArrayContains function.
        Checks if an array contains a specific value.
        """
        if not isinstance(arr, list):
            raise JSONPathError(
                "States.ArrayContains: First argument must be an array")

        # Use deep comparison for objects
        if isinstance(value, (dict, list)):
            for item in arr:
                if json.dumps(item) == json.dumps(value):
                    return True
            return False

        # Simple value comparison
        return value in arr

    @staticmethod
    def array_range(start: int, end: int, step: int = 1) -> List[int]:
        """
        Implements States.ArrayRange function.
        Creates a new array containing a range of integers.
        """
        if not all(isinstance(x, int) for x in [start, end, step]):
            raise JSONPathError(
                "States.ArrayRange: All arguments must be integers")

        if step == 0:
            raise JSONPathError("States.ArrayRange: Step cannot be zero")

        result = list(range(start, end + (1 if step > 0 else -1), step))

        if len(result) > 1000:
            raise JSONPathError(
                "States.ArrayRange: Result array cannot contain more than 1000 items")

        return result

    @staticmethod
    def array_get_item(arr: List[Any], index: int) -> Any:
        """
        Implements States.ArrayGetItem function.
        Returns the item at the specified index in an array.
        """
        if not isinstance(arr, list):
            raise JSONPathError(
                "States.ArrayGetItem: First argument must be an array")

        if not isinstance(index, int):
            raise JSONPathError(
                "States.ArrayGetItem: Second argument must be an integer")

        if index < 0 or index >= len(arr):
            raise JSONPathError(
                f"States.ArrayGetItem: Index {index} out of bounds for array of length {len(arr)}")

        return arr[index]

    @staticmethod
    def array_length(arr: List[Any]) -> int:
        """
        Implements States.ArrayLength function.
        Returns the length of an array.
        """
        if not isinstance(arr, list):
            raise JSONPathError(
                "States.ArrayLength: Argument must be an array")

        return len(arr)

    @staticmethod
    def array_unique(arr: List[Any]) -> List[Any]:
        """
        Implements States.ArrayUnique function.
        Returns an array with duplicate values removed.
        """
        if not isinstance(arr, list):
            raise JSONPathError(
                "States.ArrayUnique: Argument must be an array")

        # Handle complex objects by converting to JSON strings for comparison
        result = []
        seen = set()

        for item in arr:
            if isinstance(item, (dict, list)):
                item_json = json.dumps(item, sort_keys=True)
                if item_json not in seen:
                    seen.add(item_json)
                    result.append(item)
            else:
                if item not in seen:
                    seen.add(item)
                    result.append(item)

        return result

    @staticmethod
    def base64_encode(data: str) -> str:
        """
        Implements States.Base64Encode function.
        Encodes a string using base64 encoding.
        """
        if not isinstance(data, str):
            raise JSONPathError(
                "States.Base64Encode: Argument must be a string")

        if len(data) > 10000:
            raise JSONPathError(
                "States.Base64Encode: Input string cannot be longer than 10000 characters")

        return base64.b64encode(data.encode('utf-8')).decode('utf-8')

    @staticmethod
    def base64_decode(data: str) -> str:
        """
        Implements States.Base64Decode function.
        Decodes a base64 encoded string.
        """
        if not isinstance(data, str):
            raise JSONPathError(
                "States.Base64Decode: Argument must be a string")

        if len(data) > 10000:
            raise JSONPathError(
                "States.Base64Decode: Input string cannot be longer than 10000 characters")

        try:
            return base64.b64decode(data).decode('utf-8')
        except Exception as e:
            raise JSONPathError(f"States.Base64Decode error: {str(e)}")

    @staticmethod
    def hash(data: Any, algorithm: str) -> str:
        """
        Implements States.Hash function.
        Calculates a hash value for the input data using the specified algorithm.
        """
        if not isinstance(algorithm, str):
            raise JSONPathError(
                "States.Hash: Second argument must be a string")

        algorithm = algorithm.upper()
        valid_algorithms = {'MD5', 'SHA-1', 'SHA-256', 'SHA-384', 'SHA-512'}

        if algorithm not in valid_algorithms:
            raise JSONPathError(
                f"States.Hash: Algorithm must be one of {valid_algorithms}")

        # Convert data to JSON string if it's not already a string
        if not isinstance(data, str):
            data = json.dumps(data)

        if len(data) > 10000:
            raise JSONPathError(
                "States.Hash: Input data too large (max 10000 characters)")

        # Map algorithm names to hashlib functions
        hash_funcs = {
            'MD5': hashlib.md5,
            'SHA-1': hashlib.sha1,
            'SHA-256': hashlib.sha256,
            'SHA-384': hashlib.sha384,
            'SHA-512': hashlib.sha512
        }

        hash_func = hash_funcs[algorithm]
        return hash_func(data.encode('utf-8')).hexdigest()

    @staticmethod
    def json_merge(obj1: Dict[str, Any], obj2: Dict[str, Any], deep_merge: bool = False) -> Dict[str, Any]:
        """
        Implements States.JsonMerge function.
        Merges two JSON objects into a single object.
        """
        if not isinstance(obj1, dict) or not isinstance(obj2, dict):
            raise JSONPathError(
                "States.JsonMerge: First two arguments must be objects")

        if not isinstance(deep_merge, bool):
            raise JSONPathError(
                "States.JsonMerge: Third argument must be a boolean")

        if not deep_merge:
            # Shallow merge
            return {**obj1, **obj2}
        else:
            # Deep merge
            def deep_merge_dicts(d1, d2):
                result = d1.copy()
                for k, v in d2.items():
                    if k in result and isinstance(result[k], dict) and isinstance(v, dict):
                        result[k] = deep_merge_dicts(result[k], v)
                    else:
                        result[k] = v
                return result

            return deep_merge_dicts(obj1, obj2)

    @staticmethod
    def math_random(start: int, end: int, seed: int = None) -> int:
        """
        Implements States.MathRandom function.
        Returns a random number between start and end.
        """
        if not isinstance(start, int) or not isinstance(end, int):
            raise JSONPathError(
                "States.MathRandom: First two arguments must be integers")

        if seed is not None:
            # Use seed for deterministic results
            random.seed(seed)
            result = random.randint(start, end)
            # Reset the seed after use
            random.seed()
            return result

        return random.randint(start, end)

    @staticmethod
    def math_add(value1: int, value2: int) -> int:
        """
        Implements States.MathAdd function.
        Returns the sum of two numbers.
        """
        if not isinstance(value1, int) or not isinstance(value2, int):
            raise JSONPathError("States.MathAdd: Arguments must be integers")

        return value1 + value2

    @staticmethod
    def string_split(string: str, delimiter: str) -> List[str]:
        """
        Implements States.StringSplit function.
        Splits a string into an array of values using the specified delimiter.
        """
        if not isinstance(string, str):
            raise JSONPathError(
                "States.StringSplit: First argument must be a string")

        if not isinstance(delimiter, str):
            raise JSONPathError(
                "States.StringSplit: Second argument must be a string")

        return string.split(delimiter)

    @staticmethod
    def uuid() -> str:
        """
        Implements States.UUID function.
        Returns a v4 UUID.
        """
        return str(uuid.uuid4())


class PayloadTemplateProcessor:
    """Processes payload templates with variable substitution and intrinsic functions."""

    def __init__(self):
        self.intrinsic_functions = {
            'States.Format': JSONPathIntrinsicFunctions.format,
            'States.StringToJson': JSONPathIntrinsicFunctions.string_to_json,
            'States.JsonToString': JSONPathIntrinsicFunctions.json_to_string,
            'States.Array': JSONPathIntrinsicFunctions.array,
            'States.ArrayPartition': JSONPathIntrinsicFunctions.array_partition,
            'States.ArrayContains': JSONPathIntrinsicFunctions.array_contains,
            'States.ArrayRange': JSONPathIntrinsicFunctions.array_range,
            'States.ArrayGetItem': JSONPathIntrinsicFunctions.array_get_item,
            'States.ArrayLength': JSONPathIntrinsicFunctions.array_length,
            'States.ArrayUnique': JSONPathIntrinsicFunctions.array_unique,
            'States.Base64Encode': JSONPathIntrinsicFunctions.base64_encode,
            'States.Base64Decode': JSONPathIntrinsicFunctions.base64_decode,
            'States.Hash': JSONPathIntrinsicFunctions.hash,
            'States.JsonMerge': JSONPathIntrinsicFunctions.json_merge,
            'States.MathRandom': JSONPathIntrinsicFunctions.math_random,
            'States.MathAdd': JSONPathIntrinsicFunctions.math_add,
            'States.StringSplit': JSONPathIntrinsicFunctions.string_split,
            'States.UUID': JSONPathIntrinsicFunctions.uuid
        }

    def process_template(self, template: Any, input_data: Any, context_data: Any = None) -> Any:
        """Process a payload template by evaluating paths and intrinsic functions."""
        if context_data is None:
            context_data = {}

        if isinstance(template, dict):
            # Process dictionary template
            result = {}
            for key, value in template.items():
                if key.endswith('.$'):
                    # Path substitution
                    new_key = key[:-2]
                    if isinstance(value, str):
                        if value.startswith('$$'):
                            # Context object path
                            path_value = value[1:]  # Remove one $
                            path_result = self.evaluate_path(
                                path_value, context_data)
                            result[new_key] = path_result
                        elif value.startswith('$'):
                            # Input data path
                            path_result = self.evaluate_path(value, input_data)
                            result[new_key] = path_result
                        else:
                            # Intrinsic function
                            func_result = self.evaluate_intrinsic_function(
                                value, input_data, context_data)
                            result[new_key] = func_result
                    else:
                        # Not a string, use as is
                        result[new_key] = value
                else:
                    # Regular key, process value recursively
                    result[key] = self.process_template(
                        value, input_data, context_data)

            return result

        elif isinstance(template, list):
            # Process list template
            return [self.process_template(item, input_data, context_data) for item in template]

        else:
            # Scalar value, return as is
            return template

    def evaluate_path(self, path_str: str, data: Any) -> Any:
        """Evaluate a JSONPath expression against data."""
        # if not path_str.startswith('$'):
        #    raise JSONPathError("Path must start with $")

        lexer = JSONPathLexer(path_str)
        parser = JSONPathParser(lexer)
        operations = parser.parse()
        evaluator = JSONPathEvaluator(operations)

        results = evaluator.evaluate(data)

        # If there are multiple results, return as a list
        # If there's only one result, return it directly
        if len(results) == 0:
            return None
        elif len(results) == 1:
            return results[0]
        else:
            return results

    def evaluate_intrinsic_function(self, func_str: str, input_data: Any, context_data: Any) -> Any:
        """Evaluate an intrinsic function."""
        # Simple regex-based parser for intrinsic functions
        # In a real implementation, you'd want a more robust parser
        func_match = re.match(r'^([A-Za-z0-9_.]+)\((.*)\)', func_str)
        if not func_match:
            raise JSONPathError(
                f"Invalid intrinsic function format: {func_str}")

        func_name = func_match.group(1)
        if func_name not in self.intrinsic_functions:
            raise JSONPathError(f"Unknown intrinsic function: {func_name}")

        # Parse arguments
        args_str = func_match.group(2)
        args = []
        # Simple argument parser
        # This is a simplification; a real implementation would handle nested functions properly
        if args_str:
            in_string = False
            current_arg = ""
            string_quote = None
            i = 0

            while i < len(args_str):
                char = args_str[i]

                if not in_string and (char == "'" or char == '"'):
                    # Start of string
                    in_string = True
                    string_quote = char
                    i += 1
                    continue

                elif in_string and char == '\\' and i + 1 < len(args_str) and args_str[i + 1] == string_quote:
                    # Escaped quote in string
                    current_arg += string_quote
                    i += 2
                    continue

                elif in_string and char == string_quote:
                    # End of string
                    in_string = False
                    i += 1
                    continue

                elif not in_string and char == ',':
                    # Argument separator
                    args.append(self._parse_arg(
                        current_arg.strip(), input_data))
                    current_arg = ""
                    i += 1
                    continue

                else:
                    # Regular character
                    current_arg += char
                    i += 1

            # Add the last argument
            if current_arg.strip():
                curr = current_arg.strip()
                if curr.startswith("$$"):
                    curr = curr[1:]
                    args.append(self._parse_arg(curr, context_data))
                else:
                    args.append(self._parse_arg(curr, input_data))

        # Call the function with the parsed arguments
        func = self.intrinsic_functions[func_name]
        return func(*args)

    def _parse_arg(self, arg_str: str, input_data: Any) -> Any:
        """Parse a single argument for an intrinsic function."""
        if arg_str.startswith("'") and arg_str.endswith("'"):
            # String literal
            return arg_str[1:-1]

        elif arg_str.startswith('"') and arg_str.endswith('"'):
            # String literal with double quotes
            return arg_str[1:-1]

        elif arg_str.startswith('$'):
            # Path
            return self.evaluate_path(arg_str, input_data)

        elif arg_str.isdigit() or (arg_str.startswith('-') and arg_str[1:].isdigit()):
            # Integer
            return int(arg_str)

        elif arg_str == 'null':
            # Null value
            return None

        elif arg_str == 'true':
            # Boolean true
            return True

        elif arg_str == 'false':
            # Boolean false
            return False

        elif '(' in arg_str and ')' in arg_str:
            # Nested function
            return self.evaluate_intrinsic_function(arg_str, input_data)

        else:
            # Unknown, treat as string
            return arg_str


class JSONPath:
    """Main class for JSONPath operations."""

    def __init__(self):
        self.processor = PayloadTemplateProcessor()

    def apply(self, path: str, data: Any) -> Any:
        """Apply a JSONPath to data and return the result."""
        return self.processor.evaluate_path(path, data)

    def process_payload_template(self, template: Any, input_data: Any, context_data: Any = None) -> Any:
        """Process a payload template."""
        return self.processor.process_template(template, input_data, context_data)


# Example usage functions

def test_jsonpath_basic():
    """Test basic JSONPath functionality."""
    data = {
        "foo": 123,
        "bar": ["a", "b", "c"],
        "car": {
            "cdr": True
        }
    }

    jsonpath = JSONPath()

    # Test basic field access
    assert jsonpath.apply("$.foo", data) == 123

    # Test array access
    assert jsonpath.apply("$.bar", data) == ["a", "b", "c"]

    # Test nested field access
    assert jsonpath.apply("$.car.cdr", data) is True

    print("Basic JSONPath tests passed!")


def test_jsonpath_advanced():
    """Test more advanced JSONPath functionality."""
    data = {
        "store": {
            "book": [
                {
                    "category": "reference",
                    "author": "Nigel Rees",
                    "title": "Sayings of the Century",
                    "price": 8.95
                },
                {
                    "category": "fiction",
                    "author": "Evelyn Waugh",
                    "title": "Sword of Honour",
                    "price": 12.99
                },
                {
                    "category": "fiction",
                    "author": "Herman Melville",
                    "title": "Moby Dick",
                    "price": 8.99
                },
                {
                    "category": "fiction",
                    "author": "J. R. R. Tolkien",
                    "title": "The Lord of the Rings",
                    "price": 22.99
                }
            ],
            "bicycle": {
                "color": "red",
                "price": 19.95
            }
        }
    }

    jsonpath = JSONPath()

    # Test wildcard
    assert len(jsonpath.apply("$.store.book[*].author", data)) == 4

    # Test array slicing
    assert len(jsonpath.apply("$.store.book[0:2]", data)) == 2

    # Test recursive descent
    result = jsonpath.apply("$..price", data)
    assert len(result) == 5
    assert 8.95 in result

    print("Advanced JSONPath tests passed!")


def test_intrinsic_functions():
    """Test intrinsic functions."""
    jsonpath = JSONPath()

    # Test States.Format
    template = {
        "greeting.$": "States.Format('Hello, {}!', $.name)"
    }
    input_data = {"name": "World"}
    result = jsonpath.process_payload_template(template, input_data)
    assert result["greeting"] == "Hello, World!"

    # Test States.Array
    template = {
        "items.$": "States.Array($.item1, $.item2, $.item3)"
    }
    input_data = {"item1": 1, "item2": "two", "item3": True}
    result = jsonpath.process_payload_template(template, input_data)
    assert result["items"] == [1, "two", True]

    # Test States.StringToJson
    template = {
        "parsed.$": "States.StringToJson($.jsonString)"
    }
    input_data = {"jsonString": '{"name": "John", "age": 30}'}
    result = jsonpath.process_payload_template(template, input_data)
    assert result["parsed"]["name"] == "John"
    assert result["parsed"]["age"] == 30

    print("Intrinsic functions tests passed!")


def test_payload_template():
    """Test payload template processing."""
    jsonpath = JSONPath()

    # Setup test data
    input_data = {
        "flagged": 7,
        "vals": [0, 10, 20, 30, 40, 50]
    }

    context_data = {
        "DayOfWeek": "TUESDAY"
    }

    # Define a payload template like in the documentation
    template = {
        "flagged": True,
        "parts": {
            "first.$": "$.vals[0]",
            "last3.$": "$.vals[-3:]"
        },
        "weekday.$": "$$.DayOfWeek",
        "formattedOutput.$": "States.Format('Today is {}', $$.DayOfWeek)"
    }

    # Process the template
    result = jsonpath.process_payload_template(
        template, input_data, context_data)

    # Verify results
    assert result["flagged"] is True
    assert result["parts"]["first"] == 0
    assert result["parts"]["last3"] == [30, 40, 50]
    assert result["weekday"] == "TUESDAY", result
    assert result["formattedOutput"] == "Today is TUESDAY", result

    print("Payload template tests passed!")


def test_jsonpath_for_aws_states():
    """
    Test JSONPath functionality for AWS States simulation.
    This is a more comprehensive example that simulates a state machine transition.
    """
    jsonpath = JSONPath()

    # Sample state machine definition (simplified)
    state_machine = {
        "StartAt": "ProcessData",
        "States": {
            "ProcessData": {
                "Type": "Task",
                "InputPath": "$.data",
                "Parameters": {
                    "values.$": "$.items",
                    "operation": "sum",
                    "metadata": {
                        "timestamp.$": "States.Format('{}', $.timestamp)",
                        "source": "jsonpath-test"
                    }
                },
                "ResultPath": "$.result",
                "OutputPath": "$",
                "End": True
            }
        }
    }

    # Sample input
    input_data = {
        "data": {
            "items": [1, 2, 3, 4, 5],
            "timestamp": "2025-03-04T12:00:00Z"
        },
        "metadata": {
            "requestId": "12345"
        }
    }

    # Simulate state machine execution

    # 1. Get the current state
    current_state = state_machine["States"]["ProcessData"]

    # 2. Apply InputPath
    if "InputPath" in current_state:
        effective_input = jsonpath.apply(
            current_state["InputPath"], input_data)
    else:
        effective_input = input_data

    # 3. Apply Parameters
    if "Parameters" in current_state:
        effective_input = jsonpath.process_payload_template(
            current_state["Parameters"], effective_input)

    # 4. Simulate Task execution (here we're just summing the values)
    if effective_input["operation"] == "sum":
        task_result = sum(effective_input["values"])
    else:
        task_result = 0

    # 5. Apply ResultPath (merge task result with original input)
    if "ResultPath" in current_state:
        # A real implementation would merge properly
        # For simplicity, we'll just set the result field
        input_data_copy = input_data.copy()
        result_path = current_state["ResultPath"].replace("$.", "").split(".")

        # Navigate to the correct place in the data structure
        current = input_data_copy
        for i, part in enumerate(result_path):
            if i == len(result_path) - 1:
                current[part] = task_result
            elif part not in current:
                current[part] = {}
                current = current[part]
            else:
                current = current[part]
    else:
        input_data_copy = task_result

    # 6. Apply OutputPath
    if "OutputPath" in current_state:
        output = jsonpath.apply(current_state["OutputPath"], input_data_copy)
    else:
        output = input_data_copy

    # Verify the output
    assert "result" in output
    assert output["result"] == 15
    assert "data" in output
    assert "metadata" in output

    print("AWS States simulation test passed!")


if __name__ == "__main__":
    # Run test functions
    test_jsonpath_basic()
    test_jsonpath_advanced()
    test_intrinsic_functions()
    test_payload_template()
    test_jsonpath_for_aws_states()

    print("All tests passed! JSONPath implementation is working correctly.")
