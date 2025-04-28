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

    # Return dictionary containing configuration with secrets.
    def __get_configuration_with_secrets(self):
        # Resolve secrets and initialize dictionary the first time method is called.
        if not hasattr(self, f"_{self.__class__.__name__}__configuration_with_secrets"):
            configuration_with_secrets = {}
            for key, value in vars(self).items():
                value = resolve_secret(value)
                # Wrap value in function.
                def get_value(value=value):
                    return value
                configuration_with_secrets[key] = get_value
            self.__configuration_with_secrets = configuration_with_secrets
        return self.__configuration_with_secrets

    def test(self):
        import pprint
        pprint.pprint(self)
        pprint.pprint(self.__get_configuration_with_secrets()["ConnectionName"]())
        pprint.pprint(self.__get_configuration_with_secrets())
