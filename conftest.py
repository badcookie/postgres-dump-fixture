import pytest
import docker


@pytest.fixture
def docker_client():
    return docker.from_env()


@pytest.yield_fixture(autouse=True)
def postgresql(docker_client):
    image, _ = docker_client.images.pull('postgres')
    container = docker_client.containers.create(
        image=image, network='host', auto_remove=True
    )
    container.start()
    yield container
    container.stop()

