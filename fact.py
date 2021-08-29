"""
Atomic facts.
"""
from __future__ import annotations
import collections
from datetime import datetime
import hashlib
import logging
from uuid import uuid4

from dataclasses import dataclass
from entities import EntityType
from attribute import Attribute, Relationship

logging.basicConfig(level=logging.DEBUG)

_MESSAGE_TYPE_CLS_LIST_DICT: dict = collections.defaultdict(list)
_MESSAGE_TYPE_FUNCTION_TO_DICT: dict = collections.defaultdict(dict)

__MISSING__ = "__MISSING__"

class Fact:
    """
    Superclass for all relationships and attributes.
    """

    entity_type: Any
    attribute: Any
    entity_id: Any


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

    def __init__(
        self,
        source_entity_type: Type = None,
        source_entity_id: UUID = None,
        target_entity_type: Type = None,
        target_entity_id: UUID = None,
        relationship: Type[Relationship] = None,
    ):
        self.source_entity_type: Optional[Type] = source_entity_type
        self.source_entity_id: UUID = source_entity_id
        self.target_entity_type: Optional[Type] = target_entity_type
        self.target_entity_id: UUID = target_entity_id
        self.relationship: Optional[Type] = relationship
        self.created_at: datetime = datetime.now()
        self._validate()

    def _validate(self):
        assert self.relationship is not None, "Relationship type is `None`."
        assert (
            EntityType in self.source_entity_type.__bases__
        ), "Entity type for Relationship is not `EntityType class`."
        assert (
            EntityType in self.target_entity_type.__bases__
        ), "Entity type for Relationship is not `EntityType class`."

    def __repr__(self):
        out = (
            f"RelationshipFact: "
            f"({self.source_entity_type.__name__} ({self.source_entity_id}))"
            f"-[{self.relationship.__name__}]->"
            f"({self.target_entity_type.__name__} ({self.target_entity_id}))"
        )
        return out
