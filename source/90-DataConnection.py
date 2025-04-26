class DataConnection:

    __secrets = {}

    # Initialize connection.
    def __init__(self, configuration):
        if not isinstance(configuration, dict):
            raise Exception("Invalid configuration")
        for key, value in configuration.items():
            setattr(self, key, value)
        self.__validate_configuration()

    # Validate configuration attributes.
    def __validate_configuration(self):
        # Validate mandatory ConnectionName.
        if not hasattr(self, "ConnectionName"):
            raise Exception(f"Missing ConnectionName in {vars(self)}")
        if not isinstance(self.ConnectionName, str) or self.ConnectionName.strip() == "":
            raise Exception(f"Invalid ConnectionName in {vars(self)}")

        # Validate mandatory ConcurrencyLimit.
        if not hasattr(self, "ConcurrencyLimit"):
            raise Exception(f"Missing ConcurrencyLimit in {self.ConnectionName}")
        if not isinstance(self.ConcurrencyLimit, int) or self.ConcurrencyLimit < 1:
            raise Exception(f"Invalid ConcurrencyLimit in {self.ConnectionName}")
