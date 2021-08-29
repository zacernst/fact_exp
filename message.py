'''
messages
'''

from dataclasses import dataclass

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
        "state": [
            "cdc",
            "columns",
            "state",
        ],
    }

    relationship_mapping = {
        "LivesIn": {
            "source": {
                "entity_type": "Person",
                "entity_id_keypath": "user_id",
            },
            "target": {
                "entity_type": "State",
                "entity_id_keypath": "state",
            },
        }
    }
