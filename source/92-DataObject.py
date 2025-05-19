class DataObject:

    # Initialize object.
    def __init__(self, configuration):
        if not isinstance(configuration, dict):
            raise Exception("Invalid configuration")
        for key, value in configuration.items():
            setattr(self, key, value)
        self.__validate_configuration()
        self.__set_concurrency_number()

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

        # Validate mandatory Function.
        if not hasattr(self, "Function"):
            raise Exception(f"Missing Function in {self.ObjectName}")
        if not isinstance(self.Function, str) or self.Function.strip() == "" or not self.Function.startswith("load_"):
            raise Exception(f"Invalid Function in {self.ObjectName}")

        # Validate mandatory Status.
        if not hasattr(self, "Status"):
            raise Exception(f"Missing Status in {self.ObjectName}")
        if not isinstance(self.Status, str) or not self.Status in ["Active", "Inactive"]:
            raise Exception(f"Invalid Status in {self.ObjectName}")

        # Validate optional ConcurrencyNumber.
        if hasattr(self, "ConcurrencyNumber") and self.ConcurrencyNumber is not None and not isinstance(self.ConcurrencyNumber, int):
            raise Exception(f"Invalid ConcurrencyNumber in {self.ObjectName}")

        # Validate optional PartitionNumber.
        if hasattr(self, "PartitionNumber") and self.PartitionNumber is not None and not isinstance(self.PartitionNumber, int):
            raise Exception(f"Invalid PartitionNumber in {self.ObjectName}")

        # Validate optional Tags.
        if hasattr(self, "Tags") and self.Tags is not None and not isinstance(self.Tags, list):
            raise Exception(f"Invalid Tags in {self.ObjectName}")

    # Set concurrency number to 1 or the greatest of concurrency number and partition number.
    def __set_concurrency_number(self):
        concurrency_number = 1
        if hasattr(self, "ConcurrencyNumber") and self.ConcurrencyNumber is not None and self.ConcurrencyNumber > concurrency_number:
            concurrency_number = self.ConcurrencyNumber
        if hasattr(self, "PartitionNumber") and self.PartitionNumber is not None and self.PartitionNumber > concurrency_number:
            concurrency_number = self.PartitionNumber
        self.ConcurrencyNumber = concurrency_number

    # Inject connection.
    def set_connection(self, connection):
        if not isinstance(connection, DataConnection):
            raise Exception("Invalid connection")
        if hasattr(self, f"_{self.__class__.__name__}__connection"):
            raise Exception("Connection already set")
        self.__connection = connection

    # Return connection.
    def get_connection(self):
        return self.__connection

    # Load object.
    def load(self):
        function = globals()[self.Function]
        function_arguments = {}
        # Map object configuration to load function arguments.
        for key, value in vars(self).items():
            if value is not None:
                if key == "Mode"            : function_arguments["mode"]            = value
                if key == "ObjectName"      : function_arguments["target"]          = value
                if key == "ObjectSource"    : function_arguments["source"]          = value
                if key == "KeyColumns"      : function_arguments["key"]             = value
                if key == "ExcludeColumns"  : function_arguments["exclude"]         = value
                if key == "IgnoreColumns"   : function_arguments["ignore"]          = value
                if key == "HashColumns"     : function_arguments["hash"]            = value
                if key == "DropColumns"     : function_arguments["drop"]            = value
                if key == "BookmarkColumn"  : function_arguments["bookmark_column"] = value
                if key == "BookmarkOffset"  : function_arguments["bookmark_offset"] = value
                if key == "PartitionColumn" : function_arguments["parallel_column"] = value
                if key == "PartitionNumber" : function_arguments["parallel_number"] = value
        if self.__connection.has_reader():
            function_arguments["reader"] = self.__connection.get_reader()
        return function(**function_arguments)

    # Set loader exception.
    def set_loader_exception(self, exception):
        self.__loader_exception = exception

    # Return true if object has loader exception else return false.
    def has_loader_exception(self):
        return True if hasattr(self, f"_{self.__class__.__name__}__loader_exception") else False

    # Return loader exception.
    def get_loader_exception(self):
        return self.__loader_exception

    # Set loader duration.
    def set_loader_duration(self, duration):
        self.__loader_duration = duration

    # Return loader duration.
    def get_loader_duration(self):
        return self.__loader_duration

    # Set loader result.
    def set_loader_result(self, result):
        self.__loader_result = result

    # Return loader result.
    def get_loader_result(self):
        return self.__loader_result

    # Set loader status.
    def set_loader_status(self, status):
        self.__loader_status = status

    # Return loader status.
    def get_loader_status(self):
        return self.__loader_status
