# Parse tag string and return tag list.
def parse_tag(tag):
    if isinstance(tag, str):
        tag = tag.strip()
        tag = sub(r"\s+", ",", tag)             # Replace multiple spaces with a single comma.
        tag = sub(r",+", ",", tag)              # Replace multiple commas with a single comma.
        tag = sub(r"[^A-Za-z0-9_,]+", "", tag)  # Remove everything except alphanumeric characters, underscores and commas.
        tag = tag.split(",")
        return tag
    else:
        raise Exception("Invalid tag")

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
