import os
import shutil
import time
import requests
from logger import get_logger
from typing import Generator

logger = get_logger()


def loop(a: int, b: int):
    for i in range(a, b):
        yield 2 ** 5


def create_tmp_dir() -> Generator:
    logger.info("Start create_dir")
    yield
    os.makedirs("tmp", exist_ok=True)
    logger.info("Success create_dir")


def create_file() -> Generator:
    logger.info("Start create_file")
    yield
    logger.info("Get response in create_file")
    response = requests.get("https://ya.ru")
    yield
    logger.info("create_file: Save to file")
    with open('tmp/response.txt', 'w') as f:
        f.write(response.text)
    logger.info("Success create_file")


def delete_tmp_dir() -> Generator:
    logger.info("Start delete_dir")
    yield
    shutil.rmtree("tmp/", ignore_errors=True)
    logger.info("Success delete_dir")


def long_time_job() -> Generator:
    logger.info("Start long_time_job")
    time.sleep(3)
    yield
    logger.info("long_time_job continue")
    yield
    logger.info("Success long_time_job")


def job_with_error():
    logger.info("Start job_with_error")
    raise ValueError
