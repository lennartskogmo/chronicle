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
        # Resolve secrets and initialize secrets dictionary the first time method is called.
        if not hasattr(self, f"_{self.__class__.__name__}__secrets"):
            print(f"Init {self.ConnectionName}")
            secrets = {}
            for secret_name, secret in vars(self).items():
                secret = resolve_secret(secret)
                def get_secret():
                    return secret
                secrets[secret_name] = get_secret
            self.__secrets = secrets
        return self.__secrets

    def test(self):
        print(self.__get_configuration_with_secrets())
