import pytest

from db import dump


def dump_db(test):
    def wrapper(*args, **kwargs):
        try:
            test(*args, **kwargs)
        except AssertionError:
            dump()
            raise
    return wrapper


@dump_db
def test_pg():
    assert False
