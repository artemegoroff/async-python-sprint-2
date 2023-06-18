import unittest
from unittest.mock import MagicMock, Mock
from scheduler import Scheduler
from job import Job
from target_funcs import loop, long_time_job


class SchedulerTests(unittest.TestCase):

    def test_add_task(self):
        scheduler = Scheduler()
        job = Job(target=loop)
        scheduler.add_task(job)
        self.assertEqual(len(scheduler.queue), 1)
        self.assertEqual(scheduler.queue[0], job)

    def test_restore_tasks(self):
        scheduler = Scheduler()
        filename = 'test_tasks.pkl'
        job1 = Job(target=loop)
        job2 = Job(target=long_time_job, max_working_time=999, tries=99)
        tasks = [job1, job2]
        scheduler._save_tasks(filename, tasks)
        restored_tasks = scheduler.restore_tasks(filename)
        self.assertEqual(len(restored_tasks), 2)
        self.assertEqual(restored_tasks[0].target, job1.target)
        self.assertEqual(restored_tasks[1].target, job2.target)
        self.assertEqual(restored_tasks[1].max_working_time, job2.max_working_time)
        self.assertEqual(restored_tasks[1].tries, job2.tries)

    def test_handle_job_dependencies_not_completed(self):
        scheduler = Scheduler()
        job = Job(target=Mock)
        job.is_dependencies_completed = MagicMock(return_value=False)
        scheduler.handle_job(job)
        self.assertEqual(len(scheduler.queue), 1)
        self.assertEqual(scheduler.queue[0], job)

    def test_handle_job_start_time_not_past(self):
        scheduler = Scheduler()
        job = Job(target=Mock)
        job.is_dependencies_completed = MagicMock(return_value=True)
        job.is_start_time_past = MagicMock(return_value=False)
        scheduler.handle_job(job)
        self.assertEqual(len(scheduler.queue), 1)
        self.assertEqual(scheduler.queue[0], job)

    def test_handle_job_not_running(self):
        scheduler = Scheduler()
        scheduler.is_running = False
        job = Job(target=Mock)
        job.is_dependencies_completed = MagicMock(return_value=True)
        job.is_start_time_past = MagicMock(return_value=True)
        scheduler.handle_job(job)
        self.assertEqual(len(scheduler.not_completed_job), 1)
        self.assertEqual(scheduler.not_completed_job[0], job)

    def test_handle_job_generator_not_created(self):
        scheduler = Scheduler()
        job = Job(target=Mock)
        job.is_dependencies_completed = MagicMock(return_value=True)
        job.is_start_time_past = MagicMock(return_value=True)
        job.run = MagicMock(return_value=iter([]))
        self.assertRaises(StopIteration, scheduler.handle_job, job)

    def test_handle_job_finish_work_time(self):
        scheduler = Scheduler()
        job = Job(target=loop, args=(1, 5))
        job.is_dependencies_completed = MagicMock(return_value=True)
        job.is_start_time_past = MagicMock(return_value=True)
        job.is_finish_work_time = MagicMock(return_value=True)
        scheduler.add_task(job)
        scheduler.run()
        self.assertEqual(len(scheduler.completed_job), 1)
        self.assertEqual(scheduler.completed_job[0], job.target.__name__)

    def test_handle_job_stop_iteration(self):
        scheduler = Scheduler()
        job = Job(target=loop, args=(1, 5))
        job.is_dependencies_completed = MagicMock(return_value=True)
        job.is_start_time_past = MagicMock(return_value=True)
        job.generator = iter([1, 2, 3])
        scheduler.add_task(job)
        scheduler.run()
        self.assertEqual(len(scheduler.completed_job), 1)
        self.assertEqual(scheduler.completed_job[0], job.target.__name__)

    def test_handle_job_exception_retries(self):
        scheduler = Scheduler()
        job = Job(target=loop, args=(1, 5))
        job.is_dependencies_completed = MagicMock(return_value=True)
        job.is_start_time_past = MagicMock(return_value=True)
        job.tries = 1
        scheduler.handle_job(job)
        self.assertEqual(len(scheduler.queue), 1)
        self.assertEqual(scheduler.queue[0], job)


if __name__ == '__main__':
    unittest.main()
