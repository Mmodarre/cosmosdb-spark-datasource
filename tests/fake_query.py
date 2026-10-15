"""WHERE-clause evaluation for the fake service, with Cosmos DB for NoSQL semantics.

Supports ``SELECT * FROM <container> [[AS] alias] [WHERE <expr>]`` with property paths (``c.a``,
``c["a"]``), parameters, literals, ``AND``/``OR``/``NOT``, comparisons, ``IN``, and the functions the
filter pushdown generates. Semantics follow the service:

* a missing property is ``undefined``, and so is a comparison involving ``undefined`` or operands of
  different JSON types (numbers compare as doubles, strings ordinally);
* ``AND``, ``OR`` and ``NOT`` are three-valued over true, false and undefined (``false AND undefined``
  is false, ``true OR undefined`` is true, anything else involving undefined is undefined);
* a document is returned only if the WHERE expression is true; ``ORDER BY`` and ``OFFSET`` are ignored.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from typing import Any


class _Undefined:
    def __repr__(self) -> str:
        return "undefined"


UNDEFINED: Any = _Undefined()
_TOKEN = re.compile(
    r"""\s*(?:(?P<str>'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")|(?P<num>-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)"""
    r"""|(?P<param>@[A-Za-z_][A-Za-z0-9_]*)|(?P<word>[A-Za-z_][A-Za-z0-9_]*)|(?P<op><=|>=|!=|<>|[=<>()\[\],.*]))""",
    re.DOTALL,
)


def _kind(v: Any) -> str:
    if v is UNDEFINED:
        return "undefined"
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, (int, float)):
        return "number"
    if isinstance(v, str):
        return "string"
    if isinstance(v, list):
        return "array"
    return "object"


def _unescape(text: str) -> str:
    body = text[1:-1]
    return re.sub(r"\\(.)", lambda m: {"n": "\n", "t": "\t"}.get(m.group(1), m.group(1)), body)


def _equal(a: Any, b: Any) -> Any:
    ka, kb = _kind(a), _kind(b)
    if "undefined" in (ka, kb) or ka != kb:
        return UNDEFINED
    if ka == "number":
        return float(a) == float(b)
    return a == b


def _compare(a: Any, b: Any, op: str) -> Any:
    if op in ("=", "!=", "<>"):
        eq = _equal(a, b)
        return eq if eq is UNDEFINED or op == "=" else not eq
    ka, kb = _kind(a), _kind(b)
    if ka != kb or ka not in ("number", "string", "bool"):
        return UNDEFINED
    if ka == "number":
        a, b = float(a), float(b)
    return {"<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b}[op]


def _and(a: Any, b: Any) -> Any:
    if a is False or b is False:
        return False
    if a is True and b is True:
        return True
    return UNDEFINED


def _or(a: Any, b: Any) -> Any:
    if a is True or b is True:
        return True
    if a is False and b is False:
        return False
    return UNDEFINED


def _not(a: Any) -> Any:
    return (not a) if isinstance(a, bool) else UNDEFINED


def _string_function(name: str) -> Callable[[Any, Any], Any]:
    def call(x: Any, v: Any) -> Any:
        if not isinstance(x, str) or not isinstance(v, str):
            return UNDEFINED
        return {"STARTSWITH": x.startswith, "ENDSWITH": x.endswith, "CONTAINS": x.__contains__}[name](v)

    return call


def _array_contains(arr: Any, x: Any) -> Any:
    if not isinstance(arr, list):
        return UNDEFINED
    return any(_equal(e, x) is True for e in arr)


_FUNCTIONS: dict[str, Callable[..., Any]] = {
    "IS_DEFINED": lambda x: x is not UNDEFINED,
    "IS_NULL": lambda x: x is None,
    "IS_STRING": lambda x: _kind(x) == "string",
    "IS_NUMBER": lambda x: _kind(x) == "number",
    "IS_BOOL": lambda x: _kind(x) == "bool",
    "IS_OBJECT": lambda x: _kind(x) == "object",
    "STARTSWITH": _string_function("STARTSWITH"),
    "ENDSWITH": _string_function("ENDSWITH"),
    "CONTAINS": _string_function("CONTAINS"),
    "ARRAY_CONTAINS": _array_contains,
}
_Expr = Callable[[Mapping[str, Any]], Any]
_IGNORED_CLAUSES = ("ORDER", "OFFSET")  # documents are served in _rid order, unlimited


class _Parser:
    def __init__(self, text: str, parameters: Mapping[str, Any]) -> None:
        self.tokens: list[tuple[str, str]] = []
        pos = 0
        text = text.strip()
        while pos < len(text):
            m = _TOKEN.match(text, pos)
            if m is None or m.end() == pos:
                raise ValueError(f"fake query: cannot parse {text[pos:]!r}")
            kind = m.lastgroup or ""
            self.tokens.append((kind, m.group(kind)))
            pos = m.end()
            while pos < len(text) and text[pos].isspace():
                pos += 1
        self.i = 0
        self.parameters = parameters
        self.alias = ""

    def peek(self) -> tuple[str, str]:
        return self.tokens[self.i] if self.i < len(self.tokens) else ("end", "")

    def word(self, *words: str) -> bool:
        kind, text = self.peek()
        if kind == "word" and text.upper() in words:
            self.i += 1
            return True
        return False

    def expect(self, text: str) -> None:
        _kind, got = self.peek()
        if got.upper() != text:
            raise ValueError(f"fake query: expected {text}, got {got!r}")
        self.i += 1

    def query(self) -> _Expr:
        self.expect("SELECT")
        self.expect("*")
        self.expect("FROM")
        _, self.alias = self.peek()
        self.i += 1
        self.word("AS")
        kind, text = self.peek()
        if kind == "word" and text.upper() not in ("WHERE", *_IGNORED_CLAUSES):
            self.alias = text
            self.i += 1
        if self.peek()[0] == "end" or self.word(*_IGNORED_CLAUSES):
            return lambda doc: True
        self.expect("WHERE")
        expr = self.expr()
        if self.peek()[0] != "end" and not self.word(*_IGNORED_CLAUSES):
            raise ValueError(f"fake query: unsupported clause at {self.peek()[1]!r}")
        return expr

    def expr(self) -> _Expr:
        left = self.conjunction()
        while self.word("OR"):
            right = self.conjunction()
            left = (lambda a, b: lambda d: _or(a(d), b(d)))(left, right)
        return left

    def conjunction(self) -> _Expr:
        left = self.negation()
        while self.word("AND"):
            right = self.negation()
            left = (lambda a, b: lambda d: _and(a(d), b(d)))(left, right)
        return left

    def negation(self) -> _Expr:
        if self.word("NOT"):
            inner = self.negation()
            return lambda d: _not(inner(d))
        return self.comparison()

    def comparison(self) -> _Expr:
        left = self.primary()
        kind, text = self.peek()
        if kind == "op" and text in ("=", "!=", "<>", "<", "<=", ">", ">="):
            self.i += 1
            right = self.primary()
            return lambda d: _compare(left(d), right(d), text)
        if self.word("IN"):
            self.expect("(")
            items = [self.primary()]
            while self.peek()[1] == ",":
                self.i += 1
                items.append(self.primary())
            self.expect(")")
            return lambda d: _array_contains([item(d) for item in items], left(d))
        return left

    def primary(self) -> _Expr:
        kind, text = self.peek()
        self.i += 1
        if kind == "str":
            value = _unescape(text)
            return lambda d: value
        if kind == "num":
            number = float(text) if any(c in text for c in ".eE") else int(text)
            return lambda d: number
        if kind == "param":
            if text not in self.parameters:
                raise ValueError(f"fake query: parameter {text} is not defined")
            param = self.parameters[text]
            return lambda d: param
        if kind == "op" and text == "(":
            inner = self.expr()
            self.expect(")")
            return inner
        if kind == "op" and text == "[":
            items: list[_Expr] = []
            while self.peek()[1] != "]":
                items.append(self.primary())
                if self.peek()[1] == ",":
                    self.i += 1
            self.expect("]")
            return lambda d: [item(d) for item in items]
        if kind == "word":
            upper = text.upper()
            if upper in ("TRUE", "FALSE"):
                return (lambda v: lambda d: v)(upper == "TRUE")
            if upper == "NULL":
                return lambda d: None
            if upper in _FUNCTIONS:
                self.expect("(")
                args = [self.expr()]
                while self.peek()[1] == ",":
                    self.i += 1
                    args.append(self.expr())
                self.expect(")")
                fn = _FUNCTIONS[upper]
                return lambda d: fn(*(a(d) for a in args))
            if text == self.alias:
                return self.path()
        raise ValueError(f"fake query: unexpected {text!r}")

    def path(self) -> _Expr:
        steps: list[Any] = []
        while True:
            _kind, text = self.peek()
            if text == ".":
                self.i += 1
                steps.append(self.peek()[1])
                self.i += 1
            elif text == "[":
                self.i += 1
                key_kind, key = self.peek()
                self.i += 1
                steps.append(_unescape(key) if key_kind == "str" else int(key))
                self.expect("]")
            else:
                break

        def get(doc: Mapping[str, Any]) -> Any:
            cur: Any = doc
            for step in steps:
                found = (isinstance(step, str) and isinstance(cur, Mapping) and step in cur) or (
                    isinstance(step, int) and isinstance(cur, list) and 0 <= step < len(cur)
                )
                if not found:
                    return UNDEFINED
                cur = cur[step]
            return cur

        return get


def compile_query(query: str, parameters: list[dict[str, Any]] | None) -> Callable[[Mapping[str, Any]], bool]:
    """Return ``matches(doc)``: True if the query returns the document."""
    expr = _Parser(query, {p["name"]: p["value"] for p in parameters or []}).query()
    return lambda doc: expr(doc) is True
