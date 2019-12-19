import pytest
import docker

from db import dump


# @pytest.fixture(scope='session')
# def docker_client():
#     return docker.from_env()
#
#
# @pytest.yield_fixture(scope='session', autouse=True)
# def postgresql(docker_client):
#     image = docker_client.images.pull('postgres:latest')
#     container = docker_client.containers.create(
#         image=image, network='host', auto_remove=True, name='pg_fixture'
#     )
#     container.start()
#     yield container
#     container.stop()


# Проверка для каждого обёрнутого теста
def dump_db_at_failure(test):
    def wrapper(*args, **kwargs):
        try:
            test(*args, **kwargs)
        except AssertionError:
            dump()
            raise
    return wrapper


# Проверка для сессии
@pytest.fixture(scope="module", autouse=True)
def failure_tracking_fixture(request):
    init = request.session.testsfailed
    yield
    print('OK session check should be 0, but it is', request.session.testsfailed - init)
