class DataConnection:

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

        # Validate optional Reader.
        if hasattr(self, "Reader") and self.Reader is not None and not isinstance(self.Reader, str):
            raise Exception(f"Invalid Reader in {self.ConnectionName}")

    # Return dictionary containing configuration with secrets.
    def __get_configuration_with_secrets(self):
        # Resolve secrets and initialize dictionary the first time method is called.
        if not hasattr(self, f"_{self.__class__.__name__}__configuration_with_secrets"):
            configuration_with_secrets = {}
            for key, value in vars(self).items():
                if value is not None:
                    value = resolve_secret(value)
                    # Wrap value in function.
                    def get_value(value=value):
                        return value
                    configuration_with_secrets[key] = get_value
            self.__configuration_with_secrets = configuration_with_secrets
        return self.__configuration_with_secrets

    # Return new reader instance.
    def get_reader(self):
        if not hasattr(self, "Reader") or self.Reader is None:
            raise Exception(f"Connection {self.ConnectionName} has no reader")
        configuration_with_secrets = self.__get_configuration_with_secrets()
        reader = globals()[configuration_with_secrets["Reader"]()]
        reader_arguments = {}
        for key, value in configuration_with_secrets.items():
            if key == "Host"      : reader_arguments["host"]      = value()
            if key == "Port"      : reader_arguments["port"]      = value()
            if key == "Database"  : reader_arguments["database"]  = value()
            if key == "Username"  : reader_arguments["username"]  = value()
            if key == "Password"  : reader_arguments["password"]  = value()
            if key == "Warehouse" : reader_arguments["warehouse"] = value()
        return reader(**reader_arguments)

    # Return true if connection has reader else return false.
    def has_reader(self):
        return True if hasattr(self, "Reader") and self.Reader is not None else False
