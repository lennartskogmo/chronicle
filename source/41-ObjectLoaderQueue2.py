class ObjectLoaderQueue2:

    length    = 0
    queued    = {}
    started   = {}
    completed = {}
    connections              = {}
    connections_with_secrets = {}
    global_maximum_concurrency = 0
    global_current_concurrency = 0
    connection_maximum_concurrency = {}
    connection_current_concurrency = {}

    # Initialize queue.
    def __init__(self, concurrency, tag):
        self.global_maximum_concurrency = concurrency
        self.__populate(tag)
        #self.sort()
        #for connection_name, connection in self.queued.items():
        #    self.connection_maximum_concurrency[connection_name] = connection["ConcurrencyLimit"]
        #    self.connection_current_concurrency[connection_name] = 0
        #    self.length += len(connection["Objects"])

    # Populate queue with objects from database.
    def __populate(self, tags):
        objects = get_objects().active().tags(tags)
        connections = objects.get_connections()

        # Set object concurrency number.
        # TODO: Move this logic inside DataObject.
        for object_name, object in objects.items():
            concurrency_number = 1
            if hasattr(object, "PartitionNumber") and isinstance(object.PartitionNumber, int) and object.PartitionNumber > concurrency_number:
                concurrency_number = object.PartitionNumber
            if hasattr(object, "ConcurrencyNumber") and isinstance(object.ConcurrencyNumber, int) and object.ConcurrencyNumber > concurrency_number:
                concurrency_number = object.ConcurrencyNumber
            object.ConcurrencyNumber = concurrency_number

        # Prepare nested queued dictionary containing objects to be processed.
        queued = {}
        for connection_name in connections.keys():
            queued[connection_name] = {"Objects" : {}}
        for object in objects.values():
            queued[object.ConnectionName]["Objects"][object.ObjectName] = object
        for connection_name, connection in queued.items():
            queued[connection_name]["Objects"] = dict(sorted(connection["Objects"].items(), key=lambda item: item[1].ConcurrencyNumber, reverse=True))
        self.queued = queued

    # Return next eligible object.
    def get(self):
        # Find next eligible object.
        object_name = None
        if self.global_current_concurrency < self.global_maximum_concurrency:
            for connection_name, connection in self.queued.items():
                if self.connection_current_concurrency[connection_name] < self.connection_maximum_concurrency[connection_name]:
                    if connection["Objects"]:
                        object_name = list(connection["Objects"].keys())[0]
                        break
        # Register object as started and return object.
        if object_name:
            object = self.queued[connection_name]["Objects"].pop(object_name)
            self.started[object_name] = object
            self.connection_current_concurrency[connection_name] += 1
            self.global_current_concurrency += 1
            return object

    # Register object as completed.
    def complete(self, object):
        object_name = object["ObjectName"]
        connection_name = object["ConnectionName"]
        self.started.pop(object_name)
        self.completed[object_name] = object
        self.connection_current_concurrency[connection_name] -= 1
        self.global_current_concurrency -= 1
        self.length -= 1

    # Return true until all objects in queue have been processed.
    def not_empty(self):
        if self.length + len(self.started) > 0:
            return True

    # Sort queue so objects will be picked from connections that require the longest remaining time first.
    def sort(self):
        # Calculate score per connection.
        score = {}
        for connection_name, connection in self.queued.items():
            length = len(connection["Objects"])
            if length > 0:
                score[connection_name] = length / connection["ConcurrencyLimit"]
            else:
                score[connection_name] = 0
        # Replace queue with new queue sorted by descending connection score.
        connection_names = sorted(score, key=score.get, reverse=True)
        queued = {}
        for connection_name in connection_names:
            queued[connection_name] = self.queued[connection_name]
        self.queued = queued
