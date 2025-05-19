def get_object(object):
    repo = DataObjectRepository()
    return repo.get_object(object)

def get_objects():
    repo = DataObjectRepository()
    return repo.get_objects()
