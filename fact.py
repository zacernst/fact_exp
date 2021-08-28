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


logging.basicConfig(level=logging.DEBUG)

_MESSAGE_TYPE_CLS_LIST_DICT: dict = collections.defaultdict(list)
_MESSAGE_TYPE_FUNCTION_TO_DICT: dict = collections.defaultdict(dict)

__MISSING__ = "__MISSING__"



class FactStore:
    """
    Superclass for all back-end storage engines.
    """
    def __init__(self, *args, **kwargs):
        self.feature_functions = {}

    def put(self, _):
        """
        Abstract base class
        """
        raise NotImplementedError("`put` method must be implemented.")

    def __call__(self, fact: Fact):
        '''
        Wraps the `put` methods so we can do callbacks and side-effects.
        '''
        self.put(fact)
        # Do something
        # Call updates on dependent features and relationships


class MemoryFactStore(FactStore):
    """
    Fact store in-memory.
    """

    def __init__(self):
        self.attributes: List[AttributeFact] = []
        self.relationships: List[RelationshipFact] = []
        super().__init__()

    def _put_attribute_fact(self, attribute_fact: AttributeFact):
        """
        Won't be used by the user.
        """
        self.attributes.append(attribute_fact)

    def _put_relationship_fact(self, relationship_fact: RelationshipFact):
        """
        Also won't be used.
        """
        self.relationships.append(relationship_fact)

    def put(self, fact: Fact):
        if isinstance(fact, RelationshipFact):
            self._put_relationship_fact(fact)
        elif isinstance(fact, AttributeFact):
            self._put_attribute_fact(fact)
        else:
            raise TypeError("Tried to put a non-Fact into the store.")


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


@dataclass
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

    def __hash__(self):
        hash_str = "__".join(
            [
                self.entity_type.__class__.__name__,
                str(self.entity_id),
                self.attribute.__class__.__name__,
                str(self.value),
            ]
        )
        hexdigest = hashlib.md5(bytes(hash_str, encoding="utf8")).hexdigest()
        return int(hexdigest, 16)

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    @property
    def hash(self) -> int:
        """
        Poor hash
        """
        return self.__hash__()


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


def inductive_attribute(f):
    setattr(f, '_attribute_function', True)
    setattr(f, '_function_name', f.__name__)
    setattr(f, '_function_signature', inspect.signature(f))
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
    function_config = yaml.load(config_lines)["config"]
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
    setattr(inner, '_attribute_function', True)
    setattr(inner, '_function_name', f.__name__)
    setattr(inner, '_function_signature', inspect.signature(f))
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


@inductive_attribute
def Person__FirstNameCaps(Person__FirstName: str = ''):
    '''
    hi
    '''
    return Person__FirstName.upper()


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


class Session:
    '''
    Holds state and so on.
    '''
    def __init__(self, fact_store_cls: FactStore=MemoryFactStore, fact_store_kwargs: dict=None):
        self.fact_store = fact_store_cls(**(fact_store_kwargs or {}))

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        pass


if __name__ == "__main__":
    with Session(fact_store_cls= MemoryFactStore) as session:
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
        logging.debug(f"message_type kwargs: {kwargs}")
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
            session.fact_store(_attribute)

    function_update_callback_dict = collections.defaultdict(list)

    attribute_function_dict = {}
    obj_names = globals().keys()
    all_obj_dict = {obj_name: globals()[obj_name] for obj_name in obj_names}
    for obj_name, obj in all_obj_dict.items():
        if hasattr(obj, '_attribute_function') and obj._attribute_function:
            attribute_function_dict[obj_name] = obj
            logging.info(f'Found atrribute function: {obj_name}')
            logging.debug(f'Signature: {inspect.signature(obj)}')
            logging.debug(f'function name: {obj._function_name}')
            logging.debug(f'signature: {obj._function_signature}')

    for func_name, func in attribute_function_dict.items():
        func_name = func._function_name
        for parameter_name, parameter in func._function_signature.parameters.items():
            if parameter_name in attribute_function_dict.keys():
                logging.debug(parameter_name)
                # Set callback whenever function 'parameter_name' is called
                function_update_callback_dict[attribute_function_dict[parameter_name]].append(func)
    print(function_update_callback_dict)
