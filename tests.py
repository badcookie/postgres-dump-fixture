import os
import pytest

from db import fill_db
from conftest import dump_db_at_failure


@pytest.fixture(scope="session", autouse=True)
def db_setup(db_connection):
    fill_db(db_connection)


@dump_db_at_failure
def fake_failed_test(_):
    assert False


@dump_db_at_failure
def fake_passed_test(_):
    assert True


def test_no_dump(db_connection):
    fake_passed_test(db_connection)
    assert not os.path.exists("dump_*")


def test_failure_dump(db_connection):
    with pytest.raises(AssertionError):
        fake_failed_test(db_connection)
        assert os.path.exists("dump_*")

