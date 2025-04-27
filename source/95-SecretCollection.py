class SecretCollection: # [OK]

    # Initialize collection.
    def __init__(self, secrets):
        if not isinstance(secrets, dict):
            raise Exception("Invalid secrets")
        self.__secrets = {}
        for secret_name, secret in secrets.items():
            self.__set_secret(secret_name, secret)

    # Wrap secret in function and store it.
    def __set_secret(self, secret_name, secret):
        def get_secret():
            return secret
        self.__secrets[secret_name] = get_secret

    # Implement items dictionary interface method.
    def items(self):
        return self.__secrets.items()
