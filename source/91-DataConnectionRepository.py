class DataConnectionRepository:

    __connections = {}
    __instance    = None

    # Implement singleton behaviour.
    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance
