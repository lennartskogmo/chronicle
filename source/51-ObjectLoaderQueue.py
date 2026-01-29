class ObjectLoaderQueue:

    # Initialize queue.
    def __init__(self, concurrency, tags):
        objects     = get_objects().active().tags(tags)
        connections = objects.get_connections()
        self.length = len(objects)

        # Initialize concurrency.
        self.global_maximum_concurrency = concurrency
        self.global_current_concurrency = 0
        self.connection_maximum_concurrency = {}
        self.connection_current_concurrency = {}
        for connection_name, connection in connections.items():
            self.connection_maximum_concurrency[connection_name] = connection.ConcurrencyLimit
            self.connection_current_concurrency[connection_name] = 0

        # Initalize object dictionaries.
        queued = {}
        for connection_name, connection in connections.items():
            queued[connection_name] = {"ConcurrencyLimit" : connection.ConcurrencyLimit, "PrioritizeConnection" : connection.PrioritizeConnection, "Objects" : {}}
        for object in objects.values():
            queued[object.ConnectionName]["Objects"][object.ObjectName] = object
        for connection_name, connection in queued.items():
            queued[connection_name]["Objects"] = dict(sorted(connection["Objects"].items(), key=lambda item: item[1].ConcurrencyNumber, reverse=True))
        self.queued    = queued
        self.completed = {}
        self.started   = {}
        self.sort()

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
        object_name = object.ObjectName
        connection_name = object.ConnectionName
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
