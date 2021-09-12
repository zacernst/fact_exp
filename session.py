"""
Atomic facts.
"""
from __future__ import annotations

import collections
from dataclasses import dataclass
import inspect
import logging
from typing import Any, List, Type
from uuid import UUID

from fact import AttributeFact, RelationshipFact
from fact_store import FactStore, MemoryFactStore
from entities import *
from attribute import *
from attribute import _MESSAGE_TYPE_CLS_LIST_DICT, _MESSAGE_TYPE_FUNCTION_TO_DICT
from message import UserTableMessageType, DictAttributeMapping
from route import Route, MessageRoundabout

logging.basicConfig(level=logging.DEBUG)

#_MESSAGE_TYPE_CLS_LIST_DICT: dict = collections.defaultdict(list)
#_MESSAGE_TYPE_FUNCTION_TO_DICT: dict = collections.defaultdict(dict)

__MISSING__ = "__MISSING__"


def get_entity_parameters(func):
    """
    Inspect the signature etc.
    """
    sig = dict(func._function_signature.parameters.items())
    out = {}
    for parameter in sig.keys():
        entity_name, attribute_name = parameter.split("__")
        entity_obj = globals()[entity_name]
        attribute_obj = globals()[attribute_name]
        logging.debug(entity_obj)
        logging.debug(attribute_obj)
        out[parameter] = (
            entity_obj,
            attribute_obj,
        )
    return out




class Session:
    """
    Holds state and so on.
    """

    def __init__(
        self,
        fact_store_cls: Type = None,
        fact_store_kwargs: dict = None,
        message_roundabout: MessageRoundabout = None,
    ):
        self.fact_store = fact_store_cls(**(fact_store_kwargs or {}))
        self.message_roundabout = message_roundabout
        self.fact_store.session = self

        def _callback_dict():
            function_update_callback_dict = collections.defaultdict(list)

            attribute_function_dict = {}
            obj_names = globals().keys()
            all_obj_dict = {
                obj_name: globals()[obj_name] for obj_name in obj_names
            }
            for obj_name, obj in all_obj_dict.items():
                if (
                    hasattr(obj, "_attribute_function")
                    and obj._attribute_function
                ):
                    attribute_function_dict[obj_name] = obj
                    logging.info(f"Found attribute function: {obj_name}")
                    logging.debug(f"Signature: {inspect.signature(obj)}")
                    logging.debug(f"function name: {obj._function_name}")
                    logging.debug(f"signature: {obj._function_signature}")

            for func in attribute_function_dict.values():
                for (
                    parameter_name,
                    _,
                ) in func._function_signature.parameters.items():
                    if parameter_name in attribute_function_dict.keys():
                        logging.debug(parameter_name)
                        # Set callback
                        entity_name, attribute_name = parameter_name.split(
                            "__"
                        )
                        entity_cls = globals()[entity_name]
                        attribute_cls = globals()[attribute_name]
                        function_update_callback_dict[
                            (
                                entity_cls,
                                attribute_cls,
                            )
                        ].append(func)
            return function_update_callback_dict

        self.callback_dict = _callback_dict()

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def __call__(self, message):
        # Route sample message to identify its type.
        message_type = self.message_roundabout(message)
        logging.debug(message_type)
        logging.debug(_MESSAGE_TYPE_CLS_LIST_DICT[message_type])
        kwargs = message_type().get_kwargs(message)
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
            id_value = DictAttributeMapping.lookup_keypath(message, id_keypath)
            logging.debug(id_value)
            logging.debug(entity_type)
            _attribute = AttributeFact(
                entity_type=entity_type,
                entity_id=id_value,
                attribute=attribute_cls,
                value=value,
            )
            logging.debug(_attribute)
            session.fact_store(_attribute)

        for (
            relationship_name,
            relationship_config,
        ) in message_type.relationship_mapping.items():
            source_entity_type = globals()[
                relationship_config["source"]["entity_type"]
            ]
            target_entity_type = globals()[
                relationship_config["target"]["entity_type"]
            ]
            relationship_cls = globals()[relationship_name]
            source_entity_id = DictAttributeMapping.lookup_keypath(
                message,
                message_type.argument_mapping[
                    message_type.relationship_mapping[relationship_name][
                        "source"
                    ]["entity_id_keypath"]
                ],
            )
            target_entity_id = DictAttributeMapping.lookup_keypath(
                message,
                message_type.argument_mapping[
                    message_type.relationship_mapping[relationship_name][
                        "target"
                    ]["entity_id_keypath"]
                ],
            )
            relationship_fact = RelationshipFact(
                source_entity_type=source_entity_type,
                target_entity_type=target_entity_type,
                source_entity_id=source_entity_id,
                target_entity_id=target_entity_id,
                relationship=relationship_cls,
            )
            self.fact_store(relationship_fact)


if __name__ == "__main__":
    _message_roundabout = MessageRoundabout()

    ROUTE = Route(
        message_type=UserTableMessageType,
        keypath=[
            "metadata",
            "table",
        ],
        value_match="users",
    )
    _message_roundabout.add_route(ROUTE)

    SAMPLE_MESSAGE = {
        "cdc": {
            "columns": {
                "id": 4,
                "name": "Bob Smith",
                "age": 48,
                "state": "FL",
            }
        },
        "metadata": {"foo": 1, "bar": 2, "table": "users"},
    }

    with Session(
        fact_store_cls=MemoryFactStore, message_roundabout=_message_roundabout
    ) as session:
        session(SAMPLE_MESSAGE)
        for i in session.fact_store:
            print(i)
