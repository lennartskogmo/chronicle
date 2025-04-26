class DataConnection:

    __secrets = {}

    # Initialize connection.
    def __init__(self, configuration):
        if not isinstance(configuration, dict):
            raise Exception("Invalid configuration")
        for key, value in configuration.items():
            setattr(self, key, value)
