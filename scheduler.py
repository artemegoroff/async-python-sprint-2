import os
from collections import deque
from job import Job
import pickle


class Scheduler:
    STORAGE_FILE_STATUS = 'queue.lock'

    def __init__(self):
        self.is_running: bool = True
        self.completed_job = []
        self.not_completed_job = []
        self.queue = deque([self.add_job(job)
                            for job in self.restore_tasks(Scheduler.STORAGE_FILE_STATUS)])

    def add_task(self, job):
        self.queue.append(job)

    @staticmethod
    def restore_tasks(filename: str):
        try:
            with open(filename, 'rb') as f:
                return pickle.load(f)
        except FileNotFoundError:
            return []

    def run(self):
        job: Job
        while True:
            try:
                job = self.queue.popleft()
            except IndexError:
                os.remove(Scheduler.STORAGE_FILE_STATUS) if os.path.exists(Scheduler.STORAGE_FILE_STATUS) else None
                break

            if not job.is_dependencies_completed() or not job.is_start_time_past():
                self.add_job(job)
                continue

            if not job.generator:
                if not self.is_running:
                    self.not_completed_jobs.append(job)
                    continue

                gen = job.run()
                job.generator = gen

            try:
                if job.is_finish_work_time():
                    raise TimeoutError(f"Job {id(self)} timed out.")
                next(gen)
            except (TimeoutError, StopIteration):
                self.completed_job.append(job.target.__name__)

    def stop(self):
        self.is_running = False
        self.run()
        self._save_tasks(Scheduler.STORAGE_FILE_STATUS, self.not_completed_jobs)

    @staticmethod
    def _save_tasks(filename, jobs: list[Job]):
        with open(filename, 'wb') as f:
            pickle.dump(jobs, f, protocol=pickle.HIGHEST_PROTOCOL)
