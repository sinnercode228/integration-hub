"""Parsing of ``application/x-www-form-urlencoded`` bodies with PHP-style bracket keys.

amoCRM and Bitrix24 send webhooks as ``leads[add][0][id]=1&leads[add][0][name]=...``;
this module turns that into nested dicts/lists the rest of the code can work with.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qsl

_KEY_TOKENS = re.compile(r"\[([^\[\]]*)\]")


def split_key(key: str) -> list[str]:
    head, bracket, _ = key.partition("[")
    if not bracket:
        return [key]
    return [head, *_KEY_TOKENS.findall(key[len(head) :])]


def _listify(node: Any) -> Any:
    if isinstance(node, dict):
        converted = {key: _listify(value) for key, value in node.items()}
        if converted and all(key.isdigit() for key in converted):
            return [converted[key] for key in sorted(converted, key=int)]
        return converted
    if isinstance(node, list):
        return [_listify(item) for item in node]
    return node


def parse_form(body: bytes | str, encoding: str = "utf-8") -> dict[str, Any]:
    text = body.decode(encoding, errors="replace") if isinstance(body, bytes) else body
    root: dict[str, Any] = {}
    for raw_key, value in parse_qsl(text, keep_blank_values=True):
        tokens = split_key(raw_key)
        node: Any = root
        for index, token in enumerate(tokens):
            last = index == len(tokens) - 1
            if token == "":
                # "tags[]=a&tags[]=b" -> append semantics, stored under a synthetic index
                token = str(len(node))
            if last:
                node[token] = value
                break
            child = node.get(token)
            if not isinstance(child, dict):
                child = {}
                node[token] = child
            node = child
    result = _listify(root)
    return result if isinstance(result, dict) else {}
