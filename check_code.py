import multiprocessing
from datetime import datetime
from job import Job
from scheduler import Scheduler
from logger import get_logger

logger = get_logger()


def loop(a, b):
    for i in range(a, b):
        print(2 ** 5)
    yield


start_time = datetime.now()
condition = multiprocessing.Condition()
J1 = Job(
    # start_at=datetime(2022, 10, 19, 00, 39, 0),
    max_working_time=0,
    tries=3,
    target=loop,
    dependencies=[],
    args=(10, 10000),
)
J2 = Job(
    start_at=datetime(2023, 6, 16, 21, 13, 0),
    max_working_time=0,
    tries=3,
    target=loop,
    args=(10, 200000),
)
J3 = Job(
    start_at=datetime(2023, 6, 16, 21, 13, 0),
    max_working_time=4,
    tries=3,
    target=loop,
    args=(10, 43432),
)

scheduler = Scheduler()
scheduler.add_task(J1)
scheduler.add_task(J2)
scheduler.add_task(J3)
try:
    scheduler.run()
except KeyboardInterrupt:
    scheduler.stop()
