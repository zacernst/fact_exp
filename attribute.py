"""
Atomic facts.
"""
from __future__ import annotations

import collections
from datetime import datetime
from dataclasses import dataclass
import hashlib
import inspect
import logging
import textwrap
from typing import Any, List, Optional, Type
from uuid import UUID, uuid4
import yaml

from entities import EntityType, Person, State
from message import Message, UserTableMessageType

logging.basicConfig(level=logging.DEBUG)

_MESSAGE_TYPE_CLS_LIST_DICT: dict = collections.defaultdict(list)
_MESSAGE_TYPE_FUNCTION_TO_DICT: dict = collections.defaultdict(dict)

__MISSING__ = "__MISSING__"


class Attribute:
    """
    Superclass for all attributes.
    """

    attribute_type: Optional[Type] = None
    value: Any = __MISSING__

    def _validate(self):
        assert isinstance(self.value, self.attribute_type)


class Relationship:
    """
    Superclass for all relationships.
    """

    relationship_type: Optional[Type] = None

    def _validate(self):
        assert isinstance(self.value, self.relationship_type)


class LivesIn(Relationship):
    """
    Person lives in a State.
    """


class FirstName(Attribute):
    """
    A person's first name.
    """

    value: Optional[str] = None


class UserID(Attribute):
    """
    The ID
    """

    value: Optional[int] = None


class StateAbbreviation(Attribute):
    """
    FL, etc.
    """

    value: Optional[str] = None


class LuckyNumber(Attribute):
    """
    The person's lucky number, which is the length of their
    UserName plus their UserID.
    """

    value: Optional[int] = None


class FirstNameCaps(Attribute):
    """
    Shouting
    """

    value: Optional[str] = None


def inductive_attribute(f):
    """
    Temporary
    """
    setattr(f, "_attribute_function", True)
    setattr(f, "_function_name", f.__name__)
    setattr(f, "_function_signature", inspect.signature(f))
    return f


def attribute(f):
    """
    Decorator for attribute functions.
    """
    docstring = textwrap.dedent(f.__doc__)
    docstring_lines = docstring.split("\n")
    # Ensure that message type information is recorded

    assert any(
        i == "--" for i in docstring_lines
    ), "No config in attribute function"

    start_index = docstring_lines.index("--")
    config_lines = "\n".join(docstring_lines[start_index + 1 :])  # noqa:E203
    function_config = yaml.load(config_lines, yaml.FullLoader)["config"]
    message_type_cls_list = [
        globals()[message_type]
        for message_type in function_config["message_types"]
    ]

    entity_type_name, attribute_cls_name = f.__name__.split("__")
    entity_type = globals()[entity_type_name]
    attribute_cls = globals()[attribute_cls_name]

    def inner(**inner_kwargs):
        intersected_kwargs = {
            name: value
            for name, value in inner_kwargs.items()
            if name in inspect.getfullargspec(f).args
        }
        raw_output = f(**intersected_kwargs)
        return raw_output, attribute_cls

    for message_type_cls in message_type_cls_list:
        _MESSAGE_TYPE_CLS_LIST_DICT[message_type_cls].append(inner)

    _MESSAGE_TYPE_FUNCTION_TO_DICT[inner] = {}
    for message_type_name, id_keypath in function_config[
        "message_types"
    ].items():
        message_type_cls = globals()[message_type_name]
        _MESSAGE_TYPE_FUNCTION_TO_DICT[inner][message_type_cls] = {
            "id_keypath": id_keypath,
            "entity_type": entity_type,
        }

    # Tag the function as an attribute function
    setattr(inner, "_attribute_function", True)
    setattr(inner, "_function_name", f.__name__)
    setattr(inner, "_function_signature", inspect.signature(f))
    setattr(inner, "_callbacks", [])
    return inner


@attribute
def Person__FirstName(user_name: str = ""):
    """
    Gets the first token in the string.

    --
    config:
        message_types:
            UserTableMessageType: [cdc, columns, id]
    """
    token_list = user_name.strip().split(" ")
    return token_list[0] if len(token_list) > 0 else None


@attribute
def State__StateAbbreviation(state: str = None):
    """

    --
    config:
        message_types:
            UserTableMessageType: [cdc, columns, state]
    """
    return state.upper()


@attribute
def Person__UserID(user_id: int = None):
    """
    The user ID, which also happens to be an identifier.

    --
    config:
        message_types:
            UserTableMessageType: [cdc, columns, id]
    """
    return user_id


@inductive_attribute
def Person__FirstNameCaps(Person__FirstName: str = ""):
    """
    hi
    """
    return Person__FirstName.upper()


@attribute
def Person__LuckyNumber(user_id: str = None, user_name=None):
    """
    The user's lucky number.
    --
    config:
        message_types:
            UserTableMessageType: [cdc, columns, id]
    """
    return len(user_name) + int(user_id)
