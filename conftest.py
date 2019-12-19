import pytest
import docker


@pytest.fixture(scope='session')
def docker_client():
    return docker.from_env()


@pytest.yield_fixture(scope='session', autouse=True)
def postgresql(docker_client):
    image = docker_client.images.pull('postgres:latest')
    container = docker_client.containers.create(
        image=image, network='host', auto_remove=True
    )
    container.start()
    yield container
    container.stop()

