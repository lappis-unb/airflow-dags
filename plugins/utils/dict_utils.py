def key_lookup(obj, key):
    """
    Recursively looks up a key in a nested dictionary.

    Args:
    ----
        obj (dict): The dictionary to search.
        key (hashable): The key to look for in the dictionary.

    Returns:
    -------
        The value associated with the key if found, otherwise None.

    Examples:
    --------
        >>> data = {'a': 1, 'b': {'c': 2, 'd': {'e': 3}}}
        >>> recursive_key_lookup(data, 'e')
        3
        >>> recursive_key_lookup(data, 'x')
        None
    """
    if key in obj:
        return obj[key]

    for value in obj.values():
        if isinstance(value, dict):
            item = key_lookup(value, key)
            if item is not None:
                return item

    return None
