class DataObject:

    # Initialize object.
    def __init__(self, configuration):
        if not isinstance(configuration, dict):
            raise Exception("Invalid configuration")
        for key, value in configuration.items():
            setattr(self, key, value)
        self.__validate_configuration()
    
    # Validate configuration attributes.
    def __validate_configuration(self):
        # Validate mandatory ObjectName.
        if not hasattr(self, "ObjectName"):
            raise Exception(f"Missing ObjectName in {vars(self)}")
        if not isinstance(self.ObjectName, str) or self.ObjectName.strip() == "":
            raise Exception(f"Invalid ObjectName in {vars(self)}")

        # Validate mandatory ConnectionName.
        if not hasattr(self, "ConnectionName"):
            raise Exception(f"Missing ConnectionName in {self.ObjectName}")
        if not isinstance(self.ConnectionName, str) or self.ConnectionName.strip() == "":
            raise Exception(f"Invalid ConnectionName in {self.ObjectName}")

        # Validate mandatory Status.
        if not hasattr(self, "Status"):
            raise Exception(f"Missing Status in {self.ObjectName}")
        if not isinstance(self.Status, str) or not self.Status in ["Active", "Inactive"]:
            raise Exception(f"Invalid Status in {self.ObjectName}")

        # Validate optional Tags.
        if hasattr(self, "Tags") and not isinstance(self.Tags, list):
            raise Exception(f"Invalid Tags in {self.ObjectName}")

    def set_connection(self, connection):
        if hasattr(self, f"_{self.__class__.__name__}__connection"):
            raise Exception("Connection already set")
        self.__connection = connection

    def test(self):
        self.__connection.test()
