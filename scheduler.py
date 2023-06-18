import os
import pickle
from collections import deque
from job import Job
from logger import get_logger

logger = get_logger()


class Scheduler:
    STORAGE_FILE_STATUS = 'queue.lock'

    def __init__(self):
        self.is_running: bool = True
        self.completed_job = []
        self.not_completed_job = []
        self.queue = deque()
        [self.add_task(job)
         for job in self.restore_tasks(Scheduler.STORAGE_FILE_STATUS)]

    def add_task(self, job: Job):
        self.queue.append(job)

    @staticmethod
    def restore_tasks(filename: str):
        try:
            with open(filename, 'rb') as f:
                return pickle.load(f)
        except FileNotFoundError:
            return []

    def handle_job(self, job: Job):
        if not job.is_dependencies_completed() or not job.is_start_time_past():
            self.add_task(job)
            return

        if not job.generator:
            if not self.is_running:
                self.not_completed_job.append(job)
                return

            gen = job.run()
            job.generator = gen

        if job.is_finish_work_time():
            raise TimeoutError(f"Job {job.target.__name__} timed out.")
        next(job.generator)
        self.add_task(job)

    def run(self):
        while True:
            try:
                job: Job = self.queue.popleft()
                self.handle_job(job)
            except IndexError:
                os.remove(Scheduler.STORAGE_FILE_STATUS) if os.path.exists(Scheduler.STORAGE_FILE_STATUS) else None
                break
            except TimeoutError:
                self.completed_job.append(job.target.__name__)
                job.is_completed = True
                logger.info(f"Job {job.target.__name__} finish so long")
            except StopIteration:
                self.completed_job.append(job.target.__name__)
                job.is_completed = True
                logger.info(f"Job {job.target.__name__} finish work")
            except Exception:
                logger.exception(f"Job {job.target.__name__} is fail, attempts retries {job.tries}")
                if job.tries > 0:
                    self.add_task(job)
                continue

    def stop(self):
        self.is_running = False
        self.run()
        self._save_tasks(Scheduler.STORAGE_FILE_STATUS, self.not_completed_job)

    @staticmethod
    def _save_tasks(filename, jobs: list[Job]):
        with open(filename, 'wb') as f:
            pickle.dump(jobs, f, protocol=pickle.HIGHEST_PROTOCOL)
