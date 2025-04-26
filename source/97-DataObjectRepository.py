class DataObjectRepository:

    __collection = None
    __instance   = None

    # Implement singleton behaviour.
    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    # Initialize repository.
    def __init__(self):
        # Read data from Delta tables.
        objects = spark.table(OBJECT)
        connections = spark.table(CONNECTION).join(objects, ["ConnectionName"], "leftsemi")
        # Instantiate connections.
        connections = {row["ConnectionName"] : DataConnection(row.asDict()) for row in connections.collect()}
        # Instantiate objects.
        objects = {row["ObjectName"] : DataObject(row.asDict()) for row in objects.collect()}
        for object_name, object in objects.items():
            object.set_connection(connections[object.ConnectionName])
        # Instantiate collection.
        self.__collection = DataObjectCollection(objects)

    def get_object(self, object_name):
        return self.__collection[object_name]

    # Return collection containing all objects.
    def get_objects(self):
        return self.__collection
