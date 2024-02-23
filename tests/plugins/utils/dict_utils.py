from plugins.utils.dict_utils import key_lookup

def test_key_lookup_key_exists():
    obj = {'a': 1, 'b': {'c': 2, 'd': {'e': 3}}}
    key = 'b'
    result = key_lookup(obj, key)
    assert result == {'c': 2, 'd': {'e': 3}}

def test_key_lookup_key_does_not_exist():
    obj = {'a': 1, 'b': {'c': 2, 'd': {'e': 3}}}
    key = 'x'
    result = key_lookup(obj, key)
    assert result is None

def test_key_lookup_nested_key_exists():
    obj = {'a': 1, 'b': {'c': 2, 'd': {'e': 3}}}
    key = 'e'
    result = key_lookup(obj, key)
    assert result == 3

def test_key_lookup_nested_key_does_not_exist():
    obj = {'a': 1, 'b': {'c': 2, 'd': {'e': 3}}}
    key = 'f'
    result = key_lookup(obj, key)
    assert result is None
