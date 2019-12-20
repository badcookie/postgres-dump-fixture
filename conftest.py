import pytest
import docker
from time import sleep
from functools import wraps

import psycopg2
from psycopg2.extras import RealDictCursor

from db import dump_db, fill_db


@pytest.fixture(scope="session")
def docker_client():
    return docker.from_env()


@pytest.yield_fixture(scope="session", autouse=True)
def postgresql(docker_client):
    containers = docker_client.containers.list()
    runner_container = [*filter(lambda item: "runner" in item.name, containers)]
    network = (
        "host" if not runner_container else f"container:/{runner_container[0].name}"
    )

    image = docker_client.images.pull("postgres:latest")
    container = docker_client.containers.create(
        image=image, network=network, auto_remove=True
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def db_connection():
    sleep(5)   # Без засыпания не можем подключиться, так как база не успевает стартовать
    conn = psycopg2.connect(database="postgres", user="postgres", host="localhost")
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    yield cursor
    cursor.close()
    conn.close()


# Проверка для каждого обёрнутого теста
def dump_db_at_failure(test):
    @wraps(test)
    def wrapper(*args, **kwargs):
        connection = kwargs.get("db_connection", args[0])
        try:
            test(*args, **kwargs)
        except AssertionError:
            dump_db(connection)
            raise

    return wrapper


# # Проверка для сессии
# @pytest.fixture(scope="session", autouse=True)
# def session_failure_tracker(request, db_connection):
#     yield
#     if request.session.testsfailed:
#         dump_db(db_connection)
