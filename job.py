from datetime import datetime
from typing import Optional, Callable
from functools import wraps
from enum import Enum
from logger import get_logger

logger = get_logger()


def coroutine(func):
    # декоратор для инициализации генератора
    @wraps(func)
    def inner(*args, **kwargs):
        g = func(*args, **kwargs)
        g.send(None)
        return g

    return inner


class JobStatus(Enum):
    COMPLETED = 'completed'
    RUNNING = 'running'
    WAITING = 'waiting'
    CREATED = 'created'


class Job:
    def __init__(self,
                 target: Callable,
                 args: Optional[tuple] = None,
                 kwargs: Optional[dict] = None,
                 start_at: Optional[datetime] = None,
                 max_working_time: int = -1,
                 tries: int = 0,
                 dependencies: Optional[list] = None,
                 ):
        self.target = target
        self.__args = args or ()
        self.__kwargs = kwargs or {}
        self.start_at = start_at
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies or []
        self.is_completed = False
        self.started_at = None
        self.generator = None

    def is_start_time_past(self):
        if self.start_at:
            return datetime.now() >= self.start_at
        return True

    def is_dependencies_completed(self):
        return all(job.is_completed for job in self.dependencies)

    def is_finish_work_time(self):
        if self.max_working_time <= 0:
            return False
        working_time = (datetime.now() - self.started_at).total_seconds()
        return working_time > self.max_working_time

    @coroutine
    def run(self):
        self.tries -= 1
        logger.info(f"Job {self.target.__name__} started")
        self.started_at = datetime.now()
        yield from self.target(*self.__args, **self.__kwargs)
