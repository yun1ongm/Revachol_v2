from expression import identity


def all_equal(iterator, key=identity, default=True):
    iterator = iter(iterator)
    try:
        first = next(iterator)
    except StopIteration:
        return default
    return all(key(first) == key(x) for x in iterator)
