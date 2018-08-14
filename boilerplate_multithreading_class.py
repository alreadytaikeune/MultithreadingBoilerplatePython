#! encoding=utf8
from __future__ import unicode_literals, print_function, absolute_import
import threading
from Queue import Queue, Full, Empty

import signal


class IDontCareException(Exception):
    pass


class Executor(object):

    def __init__(self, max_job_queue_size, workers,
                 threads_args=[], daemon_threads=True,
                 max_results_queue_size=None):
        assert workers >= 1
        self.workers = workers
        self.job_queue = Queue(max_job_queue_size)
        if max_results_queue_size is not None:
            self.results_queue = Queue(max_results_queue_size)
        else:
            self.results_queue = None
        self.threads = []
        self._err_flag = False
        self._lock = threading.Lock()
        self.thread_args = threads_args
        self.daemon_threads = daemon_threads
        # signal.signal(signal.SIGINT, self.exit)

    @property
    def err_flag(self):
        with self._lock:
            return self._err_flag

    @err_flag.setter
    def err_flag(self, v):
        with self._lock:
            self._err_flag = v

    def raise_err_flag(self):
        # maybe do some logging?
        self.err_flag = True

    def exit(self):
        self.err_flag = True

    def _start_threads(self):
        assert len(self.threads) == 0
        for w in range(self.workers):
            t = threading.Thread(
                target=self._job_loop,
                args=tuple(self.thread_args))
            if self.daemon_threads:
                t.daemon = True
            self.threads.append(t)
            t.start()

    def get_from_queue(self, queue):
        while True:
            try:
                return queue.get(timeout=0.1)
            except Empty:
                print("starvation")
                if self.err_flag:
                    raise RuntimeError("Error flag raised")

    def put_in_queue(self, queue, v):
        while True:
            try:
                return queue.put(v, timeout=0.1)
            except Full:
                if self.err_flag:
                    raise RuntimeError("Error flag raised")

    def _do_job(self):
        raise NotImplementedError()

    def prepare_job_args(self, job, *thread_args):
        if not isinstance(job, list) and not isinstance(job, tuple):
            return [job]

    def prepare_result(self, job, result, *thread_args):
        return result

    def _job_loop(self, *args):

        while True:
            if self.err_flag:
                break
            job = self.get_from_queue(self.job_queue)
            if job is None:
                self.job_queue.task_done()
                return
            try:
                job_args = self.prepare_job_args(job, *args)
                res = self._do_job(*job_args)
                if self.results_queue is not None:
                    result = self.prepare_result(job, res, *args)
                    self.put_in_queue(self.results_queue, result)
                self.job_queue.task_done()
            except IDontCareException as e:
                # depending on the type of exception, you might just want to
                # ignore it...
                self.job_queue.task_done()
            except Exception as e:
                # ...or raise.
                self.job_queue.task_done()
                self.raise_err_flag()
                raise e

    def queue_end_of_job(self):
        for _ in range(self.workers):
            self.put_in_queue(self.job_queue, None)

    def join(self):
        for t in self.threads:
            t.join()

    def _execute(self, job_generator):
        for job in job_generator:
            self.put_in_queue(self.job_queue, job)
            if self.err_flag:
                break
        self.queue_end_of_job()

    def execute(self, job_generator):
        """
        Executes in parallel the jobs yielded by job_generator.
        """
        excpt = None
        self._start_threads()
        try:
            self._execute(job_generator)
        except Exception as e:
            # catch exception in the main thread.
            self.raise_err_flag()
            excpt = e
        self.join()
        if excpt is not None:
            raise excpt
        return not self.err_flag
