class ObjectLoaderQueue:

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
        self.sort()
        for connection_name, connection in self.queued.items():
            self.connection_maximum_concurrency[connection_name] = connection["ConcurrencyLimit"]
            self.connection_current_concurrency[connection_name] = 0
            self.length += len(connection["Objects"])

    # Populate queue with objects from database.
    def __populate(self, tag):
        if isinstance(tag, str):
            tag = parse_tag(tag)
        if not isinstance(tag, list):
            raise Exception("Invalid tag")
        objects = spark.table(OBJECT).withColumn("__Tags", lit(tag)).where("Status = 'Active'").filter(arrays_overlap("Tags", "__Tags")).drop("__Tags")
        connections = spark.table("__chronicle.connection").join(objects, ["ConnectionName"], "leftsemi")
        objects = {row["ObjectName"] : row.asDict() for row in objects.collect()}
        connections = {row["ConnectionName"] : row.asDict() for row in connections.collect()}
        # Set object concurrency number.
        for object_name, object in objects.items():
            concurrency_number = 1
            if "ConcurrencyNumber" in object and isinstance(object["ConcurrencyNumber"], int) and object["ConcurrencyNumber"] > concurrency_number:
                concurrency_number = object["ConcurrencyNumber"]
            if "PartitionNumber" in object and isinstance(object["PartitionNumber"], int) and object["PartitionNumber"] > concurrency_number:
                concurrency_number = object["PartitionNumber"]
            objects[object_name]["ConcurrencyNumber"] = concurrency_number
        # Prepare dictionaries containing connection details with and without secrets.
        for connection_name, connection in connections.items():
            self.connections[connection_name] = {}
            self.connections_with_secrets[connection_name] = {}
            for key, value in connection.items():
                self.connections[connection_name][key] = value
                self.connections_with_secrets[connection_name][key] = resolve_secret(value)
        # Prepare nested queued dictionary containing objects to be processed.
        for connection_name in connections.keys():
            connections[connection_name]["Objects"] = {}
        for object in objects.values():
            connections[object["ConnectionName"]]["Objects"][object["ObjectName"]] = object
        for connection_name, connection in connections.items():
            connections[connection_name]["Objects"] = dict(sorted(connection["Objects"].items(), key=lambda item: item[1]['ConcurrencyNumber'], reverse=True))
        self.queued = connections

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
            if length := len(connection["Objects"]) > 0:
                score[connection_name] = length / connection["ConcurrencyLimit"]
            else:
                score[connection_name] = 0
        # Replace queue with new queue sorted by descending connection score.
        connection_names = sorted(score, key=score.get, reverse=True)
        queued = {}
        for connection_name in connection_names:
            queued[connection_name] = self.queued[connection_name]
        self.queued = queued
