# Apply column and row filters if present, else act as pass-through function and return unaltered data frame.
def filter(df, exclude=None, where=None):
    if where is not None:
        df = df.where(where)
    if exclude is not None:
        df = exclude_columns(df, exclude)
    return df

# Exclude columns from data frame.
# Used by reader to remove columns before reading from source.
def exclude_columns(df, exclude):
    if isinstance(exclude, str):
        return df.drop(exclude)
    elif isinstance(exclude, list):
        return df.drop(*exclude)
    else:
        raise Exception("Invalid exclude")

# Drop columns from data frame.
# Used by writer to remove columns before writing to target.
def drop_columns(df, drop):
    if isinstance(drop, str):
        drop = conform_column_name(drop)
        return df.drop(drop)
    elif isinstance(drop, list):
        drop = [conform_column_name(c) for c in drop]
        return df.drop(*drop)
    else:
        raise Exception("Invalid drop")

# Add CHECKSUM column to beginning of data frame.
def add_checksum_column(df, ignore=None):
    if ignore is None:
        return df.select([md5(concat_ws("<|^|>", *sorted(df.columns))).alias(CHECKSUM), "*"])
    elif isinstance(ignore, str):
        ignore = conform_column_name(ignore)
        return df.select([md5(concat_ws("<|^|>", *sorted(c for c in df.columns if c != ignore))).alias(CHECKSUM), "*"])
    elif isinstance(ignore, list):
        ignore = [conform_column_name(c) for c in ignore]
        return df.select([md5(concat_ws("<|^|>", *sorted(c for c in df.columns if c not in ignore))).alias(CHECKSUM), "*"])
    else:
        raise Exception("Invalid ignore")

# Add KEY column to beginning of data frame.
def add_key_column(df, key):
    if isinstance(key, str):
        return df.select([df[conform_column_name(key)].alias(KEY), "*"])
    elif isinstance(key, list):
        key = sorted([conform_column_name(c) for c in key])
        return df.select([concat_ws("-", *key).alias(KEY), "*"])
    else:
        raise Exception("Invalid key")

# Add hash columns to end of data frame.
def add_hash_columns(df, hash):
    if isinstance(hash, str):
        hash = conform_column_name(hash)
        df = df.withColumn(hash+"_hash", md5(col(hash).cast(StringType())))
    elif isinstance(hash, list):
        hashes = [conform_column_name(c) for c in hash]
        for hash in hashes:
            df = df.withColumn(hash+"_hash", md5(col(hash).cast(StringType())))
    else:
        raise Exception("Invalid hash")
    return df

# Conform data frame column names to naming convention and check for duplicate column names.
def conform_column_names(df):
    df = df.toDF(*[conform_column_name(c) for c in df.columns])
    duplicates = {c for c in df.columns if df.columns.count(c) > 1}
    if duplicates:
        raise Exception(f"Duplicate column name(s): {duplicates}")
    return df

# Conform column name to naming convention.
def conform_column_name(cn):
    cn = cn.lower()                                # Convert to lowercase.
    cn = cn.translate(str.maketrans("æøå", "eoa")) # Replace Norwegian characters with Latin characters.
    cn = sub("[^a-z0-9]+", "_", cn)                # Replace all characters except letters and numbers with underscores.
    cn = sub("^_|_$", "", cn)                      # Remove leading and trailing underscores.
    return cn
