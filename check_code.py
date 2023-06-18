from datetime import datetime, timedelta

from job import Job
from scheduler import Scheduler
from target_funcs import loop, long_time_job, create_file, create_tmp_dir, delete_tmp_dir, job_with_error


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
