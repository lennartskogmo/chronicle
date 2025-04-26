class DataObjectRepository: # [OK]

    __collection = None
    __instance   = None

    # Implement singleton behaviour.
    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    # Initialize repository.
    def __init__(self):
        repo = DataConnectionRepository()
        # Read data from Delta table.
        objects = spark.table(OBJECT)
        # Instantiate objects.
        objects = {row["ObjectName"] : DataObject(row.asDict()) for row in objects.collect()}
        for object_name, object in objects.items():
            object.set_connection(repo.get_connection(object.ConnectionName))
        # Instantiate collection.
        self.__collection = DataObjectCollection(objects)

    # Return object or None if object does not exist.
    def get_object(self, object_name):
        if not isinstance(object_name, str) or object_name.strip() == "":
            raise Exception("Invalid object name")
        return self.__collection[object_name]

    # Return collection containing all objects.
    def get_objects(self):
        return self.__collection
