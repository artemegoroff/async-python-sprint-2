import os
import shutil
import time
import requests
from datetime import datetime, timedelta
from typing import Generator
from job import Job
from scheduler import Scheduler
from logger import get_logger

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


def start_scheduler():
    scheduler = Scheduler()

    job1 = Job(
        tries=3,
        target=loop,
        dependencies=[],
        args=(10, 10000),
    )
    job2 = Job(
        target=long_time_job, start_at=datetime.now() + timedelta(seconds=5),
        max_working_time=2
    )
    job3 = Job(target=create_tmp_dir)
    job4 = Job(target=create_file, dependencies=[job3])
    job5 = Job(target=delete_tmp_dir, dependencies=[job3, job4])
    job6 = Job(target=job_with_error, tries=4)

    scheduler.add_task(job1)
    scheduler.add_task(job5)
    scheduler.add_task(job4)
    scheduler.add_task(job2)
    scheduler.add_task(job3)
    scheduler.add_task(job6)
    scheduler.run()


if __name__ == "__main__":
    start_scheduler()
