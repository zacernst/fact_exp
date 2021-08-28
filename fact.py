"""
Atomic facts.

"""

import collections
from datetime import datetime
from dataclasses import dataclass
import inspect
import logging
import textwrap
from typing import Any, List, Optional, Type
from uuid import UUID, uuid4

import yaml


logging.basicConfig(level=logging.INFO)

_MESSAGE_TYPE_CLS_LIST_DICT: dict = collections.defaultdict(list)
_MESSAGE_TYPE_FUNCTION_TO_DICT: dict = collections.defaultdict(dict)

__MISSING__ = "__MISSING__"


class Message:
    """
    Superclass for all message types.
    """

    argument_mapping: dict

    def get_kwargs(self, message: dict):
        """
        Gets the kwargs
        """
        arg_map = DictAttributeMapping(keypath_arg_dict=self.argument_mapping)
        message_kwargs = arg_map(message)
        return message_kwargs


@dataclass
class UserTableMessageType(Message):
    """
    One table
    """

    argument_mapping = {
        "user_id": [
            "cdc",
            "columns",
            "id",
        ],
        "user_name": [
            "cdc",
            "columns",
            "name",
        ],
    }


class EntityType:
    """
    Mix-in for all entities.
    """


class Attribute:
    """
    Superclass for all attributes.
    """

    attribute_type: Optional[Type] = None
    value: Any = __MISSING__

    def _validate(self):
        assert isinstance(self.value, self.attribute_type)


class Person(EntityType):
    """
    A person.
    """


class FirstName(Attribute):
    """
    A person's first name.
    """

    entity_types = [
        Person,
    ]
    value: Optional[str] = None


class Fact:
    """
    Superclass for all relationships and attributes.
    """


class AttributeFact(Fact):
    """
    The main class.
    """

    def __init__(
        self,
        entity_type: Type = None,
        entity_id: UUID = None,
        attribute: Type[Attribute] = None,
        value: Any = None,
    ):
        self.entity_type: Optional[Type] = entity_type
        self.entity_id: Optional[UUID] = entity_id
        self.attribute: Optional[Type] = attribute
        self.value = value
        self.uuid: UUID = uuid4()
        self.created_at: datetime = datetime.now()
        self._validate()

    def _validate(self):
        assert self.entity_type is not None, "Entity type is `None`."
        assert (
            EntityType in self.entity_type.__bases__
        ), "Entity type is not `EntityType class`."

    def __repr__(self):
        s = (
            f"AttributeFact: "
            f"{self.entity_type.__name__}[{self.attribute.__name__}] = "
            f"{self.value} ({self.entity_id})"
        )
        return s

    @property
    def hash(self):
        """
        Poor hash
        """
        return "fake_hash"


class RelationshipFact(Fact):
    """
    Fact for a relationship between entities.
    """


class DictAttributeMapping:
    """
    keys are mapped to keypaths; the argument name is the key; where to find
    the value is the keypath.
    """

    def __init__(self, keypath_arg_dict: dict = None):
        self.keypath_arg_dict = keypath_arg_dict or {}

    @staticmethod
    def lookup_keypath(dictionary, keypath):
        """
        Traverses the keypath and returns the value at the end.
        """
        obj = dictionary
        for key in keypath:
            obj = obj.get(key, {})
        return obj

    def __call__(self, dictionary):
        key_values = {
            key: DictAttributeMapping.lookup_keypath(dictionary, keypath)
            for key, keypath in self.keypath_arg_dict.items()
        }
        return key_values


def name_length_plus_user_id(user_id=None, user_name=None):
    """
    Here is a description. Config is below the `--`
    """
    return len(user_name) + user_id


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
    function_config = yaml.load(config_lines)["config"]
    message_type_cls_list = [
        globals()[message_type]
        for message_type in function_config["message_types"]
    ]
    attribute_cls = globals()[function_config["attribute"]]
    entity_type = globals()[function_config["entity_type"]]

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

    return inner


@attribute
def first_name(user_name: str = ""):
    """
    Gets the first token in the string.

    --
    config:
        message_types:
            UserTableMessageType: [cdc, columns, id]
        attribute: FirstName
        entity_type: Person
    """
    token_list = user_name.strip().split(" ")
    return token_list[0] if len(token_list) > 0 else None


@dataclass
class Route:
    """
    Information necessary to route a message to the correct transform.
    """

    message_type: Type
    keypath: list
    value_match: Any

    def __call__(self, message):
        return (
            self.message_type
            if DictAttributeMapping.lookup_keypath(message, self.keypath)
            == self.value_match  # noqa:W503
            else None
        )


class MessageRoundabout:
    """
    Takes a message and assigns a type to it.
    """

    def __init__(self):
        self.routes: List = []

    def add_route(self, route: Route = None):
        """
        Appends a route.
        """
        self.routes.append(route)

    def __call__(self, message):
        """
        Returns the message type based on matching route.
        """
        message_type_list = [
            route(message)
            for route in self.routes
            if route(message) is not None
        ]
        assert (
            len(message_type_list) < 2
        ), "Message matched more than one route."
        assert len(message_type_list) > 0, "Message matched no routes."
        return message_type_list[0]


if __name__ == "__main__":
    message_roundabout = MessageRoundabout()
    ROUTE = Route(
        message_type=UserTableMessageType,
        keypath=[
            "metadata",
            "table",
        ],
        value_match="users",
    )
    message_roundabout.add_route(ROUTE)
    SAMPLE_MESSAGE = {
        "cdc": {
            "columns": {"id": 4, "name": "Bob Smith", "age": 48, "state": "FL"}
        },
        "metadata": {"foo": 1, "bar": 2, "table": "users"},
    }

    # Route sample message to identify its type.
    message_type = message_roundabout(SAMPLE_MESSAGE)
    logging.debug(message_type)
    logging.debug(_MESSAGE_TYPE_CLS_LIST_DICT[message_type])
    kwargs = message_type().get_kwargs(SAMPLE_MESSAGE)
    logging.debug(kwargs)
    for attribute_function in _MESSAGE_TYPE_CLS_LIST_DICT[message_type]:
        value, attribute_cls = attribute_function(**kwargs)
        logging.debug(value)
        id_keypath = _MESSAGE_TYPE_FUNCTION_TO_DICT[attribute_function][
            message_type
        ]["id_keypath"]
        entity_type = _MESSAGE_TYPE_FUNCTION_TO_DICT[attribute_function][
            message_type
        ]["entity_type"]
        id_value = DictAttributeMapping.lookup_keypath(
            SAMPLE_MESSAGE, id_keypath
        )
        logging.debug(id_value)
        print(entity_type)
        _attribute = AttributeFact(
            entity_type=entity_type,
            entity_id=id_value,
            attribute=attribute_cls,
            value=value,
        )
        print(_attribute)
