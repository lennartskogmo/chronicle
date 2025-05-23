# Return connection.
def get_connection(connection):
    repo = DataConnectionRepository()
    return repo.get_connection(connection)

# Return object.
def get_object(object):
    repo = DataObjectRepository()
    return repo.get_object(object)

# Return object collection.
def get_objects():
    repo = DataObjectRepository()
    return repo.get_objects()

# Return reader.
def get_reader(connection):
    repo = DataConnectionRepository()
    connection = repo.get_connection(connection)
    if connection is not None:
        return connection.get_reader()
    else:
        return None
