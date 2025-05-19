# Parse tags string and return tags list.
def parse_tags(tags):
    if isinstance(tags, str):
        tags = tags.strip()
        tags = sub(r"\s+", ",", tags)             # Replace multiple spaces with a single comma.
        tags = sub(r",+", ",", tags)              # Replace multiple commas with a single comma.
        tags = sub(r"[^A-Za-z0-9_,]+", "", tags)  # Remove everything except alphanumeric characters, underscores and commas.
        tags = tags.split(",")
        return tags
    else:
        raise Exception("Invalid tags")

# Return secret if value contains reference to secret, otherwise return value.
def resolve_secret(value):
    if isinstance(value, str) and value.startswith("<") and value.endswith(">"):
        if PARAMETER_STORE is not None:
            return SSM_CLIENT.get_parameter(Name=value[1:-1], WithDecryption=True)['Parameter']['Value']
        else:
            return dbutils.secrets.get(scope="kv", key=value[1:-1])
    else:
        return value

# Validate mode against dynamic list of values.
def validate_mode(valid, mode):
    if mode not in valid:
        raise Exception("Invalid mode")
