class DataObjectCollection:

    # Initialize collection.
    def __init__(self, objects):
        if not isinstance(objects, dict):
            raise Exception("Invalid objects")
        for object_name, object in objects.items():
            if not isinstance(object, DataObject):
                raise Exception(f"Invalid object {object_name}")
        self.__objects = objects

    def __getitem__(self, object_name):
        return self.__objects.get(object_name)

    def __len__(self):
        return len(self.__objects)

    def __repr__(self):
        return "\n".join([str(key) for key in self.__objects.keys()])

    def items(self):
        return self.__objects.items()

    def keys(self):
        return self.__objects.keys()

    def values(self):
        return self.__objects.values()

    def list(self):
        print(len(self))
        print(self)

    # Return new subcollection containing only active objects.
    def active(self):
        objects = {}
        for object_name, object in self.__objects.items():
            if object.Status == "Active":
                objects[object_name] = object
        return self.__class__(objects)

    # Return new subcollection containing only inactive objects.
    def inactive(self):
        objects = {}
        for object_name, object in self.__objects.items():
            if object.Status == "Inactive":
                objects[object_name] = object
        return self.__class__(objects)

    # Return new subcollection containing only objects with matching connection name.
    def connection(self, connection_name):
        if not isinstance(connection_name, str):
            raise Exception("Invalid connection name")
        objects = {}
        for object_name, object in self.__objects.items():
            if object.ConnectionName == connection_name:
                objects[object_name] = object
        return self.__class__(objects)

    # Return new subcollection containing only objects with atleast one matching tag.
    def tags(self, tags):
        if isinstance(tags, str):
            tags = parse_tag(tags)
        if not isinstance(tags, list):
            raise Exception("Invalid tags")
        objects = {}
        for object_name, object in self.__objects.items():
            if hasattr(object, "Tags") and any(item in object.Tags for item in tags):
                objects[object_name] = object
        return self.__class__(objects)
