import os
import pytest
from db import setup_db
from conftest import dump_db_at_failure


# @dump_db_at_failure
def test_failure_dump(db_connection):
    setup_db(db_connection)
    assert False


# @dump_db_at_failure
# def test_session_dump(db_connection):
#     with pytest.raises(AssertionError):
#         fail_test_and_dump(db_connection)
#
#         from db import DIRNAME
#         assert os.path.exists(DIRNAME)
