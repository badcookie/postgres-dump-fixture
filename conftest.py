import pytest
import docker
from time import sleep
from functools import wraps

import psycopg2
from psycopg2.extras import RealDictCursor

from db import dump_db


@pytest.fixture(scope="session")
def docker_client():
    return docker.from_env()


@pytest.yield_fixture(scope="session", autouse=True)
def postgresql(docker_client):
    image = docker_client.images.pull("postgres:latest")
    container = docker_client.containers.create(
        image=image, network="host", auto_remove=True
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
        connection = kwargs.get("db_connection")
        try:
            test(*args, **kwargs)
        except AssertionError:
            dump_db(connection)
            raise

    return wrapper


# Проверка для сессии
@pytest.fixture(scope="session", autouse=True)
def session_failure_tracker(request, db_connection):
    yield
    if request.session.testsfailed:
        dump_db(db_connection)
