# Convert string to list of uniquely spelled words.
def string_to_list(words):
    if isinstance(words, str):
        words = words.strip()
        words = sub(r"\s+", ",", words)             # Replace multiple spaces with a single comma.
        words = sub(r",+", ",", words)              # Replace multiple commas with a single comma.
        words = sub(r"[^A-Za-z0-9_,]+", "", words)  # Remove everything except alphanumeric characters, underscores and commas.
        words = words.split(",")                    # Convert to list.
        words = [word for word in words if word]    # Remove empty items.
        words = list(dict.fromkeys(words))          # Remove duplicate items.
        return words
    else:
        raise Exception("Invalid words")

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
