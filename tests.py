from conftest import dump_db_at_failure


@dump_db_at_failure
def test_1():
    assert True


@dump_db_at_failure
def test_2():
    assert False
