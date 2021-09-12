"""
facts
"""
from fact import Fact, AttributeFact, RelationshipFact
from entities import Person, EntityType
from attribute import Attribute, FirstName, FirstNameCaps
from fact import AttributeFact
from typing import Any, List

import logging

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


class FactStore:
    """
    Superclass for all back-end storage engines.
    """

    def __init__(self, feature_functions: dict = None):
        self.feature_functions = feature_functions or {}
        self.fact_store = None
        self.session: Any = None

    def put(self, _):
        """
        Abstract base class
        """
        raise NotImplementedError("`put` method must be implemented.")

    def __call__(self, fact: Fact):
        """
        Wraps the `put` methods so we can do callbacks and side-effects.
        """
        self.put(fact)
        # Call updates on dependent features and relationships
        callbacks = (
            self.session.callback_dict[
                (
                    fact.entity_type,
                    fact.attribute,
                )
            ]
            if isinstance(fact, AttributeFact)
            else []
        )
        for callback in callbacks:
            entity_parameters = get_entity_parameters(callback)
            callback_attrs = {
                parameter: self.get_attribute(
                    entity_type=fact.entity_type,
                    attribute=fact.attribute,
                    entity_id=fact.entity_id,
                )
                for parameter in entity_parameters
            }
            if any(value == __MISSING__ for value in callback_attrs.values()):
                logging.debug("Missing at least one parameter in callback.")
                continue

            callback_kwargs = {
                key: attribute.value
                for key, attribute in callback_attrs.items()
            }
            callback_value = callback(**callback_kwargs)
            entity_type_name, attribute_cls_name = callback.__name__.split(
                "__"
            )
            entity_type = globals()[entity_type_name]
            attribute_cls = globals()[attribute_cls_name]
            fact_from_callback = AttributeFact(
                entity_type=entity_type,
                attribute=attribute_cls,
                value=callback_value,
                entity_id=fact.entity_id,
            )
            self.put(fact_from_callback)
            logging.debug(fact_from_callback)

    def _get_attribute(
        self,
        entity_type: EntityType = None,
        attribute: Attribute = None,
        entity_id: str = None,
    ):
        raise NotImplementedError(
            "`FactStore` subclasses must implement `_get_attribute`"
        )

    def get_attribute(
        self,
        entity_type: EntityType = None,
        attribute: Attribute = None,
        entity_id: str = None,
    ):
        '''
        Calls ``_get_attribute``, which has to be provided in the child class.
        '''
        return self._get_attribute(
            entity_type=entity_type, attribute=attribute, entity_id=entity_id
        )


class MemoryFactStore(FactStore):
    """
    Fact store in-memory.
    """

    def __init__(self):
        self.attributes: List[AttributeFact] = []
        self.relationships: List[RelationshipFact] = []
        self.session = None
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
        if "RelationshipFact" in fact.__class__.__name__:
            self._put_relationship_fact(fact)
        elif "AttributeFact" in fact.__class__.__name__:
            self._put_attribute_fact(fact)
        else:
            raise TypeError("Tried to put a non-Fact into the store.")

    def _get_attribute(
        self,
        entity_type: EntityType = None,
        attribute: Attribute = None,
        entity_id: str = None,
    ):
        for fact in self.attributes:
            if (
                fact.entity_type is entity_type
                and fact.entity_id == entity_id
                and fact.attribute is attribute
            ):
                return fact
        logging.debug("no fact found")
        return __MISSING__

    def __iter__(self):
        for attribute in self.attributes:
            yield attribute
        for relationship in self.relationships:
            yield relationship
