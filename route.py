'''
Classes for routing messages
'''
from dataclasses import dataclass
from message import DictAttributeMapping
from typing import Any, Type, List


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

