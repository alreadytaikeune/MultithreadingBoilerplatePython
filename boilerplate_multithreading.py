#! encoding=utf8
from __future__ import unicode_literals, print_function, absolute_import
import threading
from Queue import Queue, Full, Empty

"""
Goal: I have a set of jobs I want to execute. There are no dependencies between
them and can be executed in any order. How do I parallelize them? I don't
necessarily have access to all the jobs from the start, nor do I want to store
them all in memory.

Usual question when writing multi-threaded programs based on a task queue:
- Synchronization
- Communication
- How to cleanly exit threads?
- How to handle exceptions in the main thread and worker thread to prevent the
program from getting indefinitely stuck?
- How to keep threads from keeping the main process alive? Are deamon threads
the panacea?
- How do I increase the chances that an interrupt signal actully interrupts my
program?

Remarks
=======
You easily rewrite this standalone script with an executor class.
Here the job generation takes place on the main thread, you can rewrite this
bit to also parallelize the job generation if needed.
"""


ERROR_FLAG = False
LOCK = threading.Lock()
WORKERS = 4
THREADS = []

MAX_JOB_QUEUE_SIZE = 100
# if nothing consumes from the results queue, you might to make it infinite
MAX_RESULTS_QUEUE_QIZE = -1
JOB_QUEUE = Queue(MAX_JOB_QUEUE_SIZE)

# set to None if you don't want to store results
RESULTS_QUEUE = Queue(MAX_RESULTS_QUEUE_QIZE)


"""
Implements a kind of semaphore mechanism to communicate an error accross
threads.
"""


def get_err_flag():
    global LOCK
    global ERROR_FLAG
    with LOCK:
        return ERROR_FLAG


def raise_err_flag():
    global LOCK
    global ERROR_FLAG
    with LOCK:
        ERROR_FLAG = True


"""
Helper methods to prevent the prevent the threads from being indefinitely stuck
on a get or put call, because the threads that fill/consume from the queue
exited on an error. On the other hand, if there is no data but no error, we
don't want to exit, rather continue until more arrives (and respectively for
the job generation, we want to wait until some jobs complete to put some new
ones).
We periodically check if the synchronized error flag is set. If not, continue
waiting.
"""


def get_from_queue(queue):
    while True:
        try:
            return queue.get(timeout=0.1)
        except Empty:
            if get_err_flag():
                raise RuntimeError("Error flag raised")


def put_in_queue(queue, v):
    while True:
        try:
            return queue.put(v, timeout=0.1)
        except Full:
            if get_err_flag():
                raise RuntimeError("Error flag raised")


"""
Job's lifecyle
"""


def job_generator():
    """
    A generator for the jobs. Since threads have a shared memory, the jobs can
    be objects, and there is no need for communication schemes.
    """
    pass


def _do_job():
    """
    This is the function that actually executes a job.

    The important part is that most of this function's work takes place in a
    GIL-free environment, otherwise they won't be any performance improvement.
    """
    pass


class IDontCareException(Exception):
    pass


def job_loop(*args):
    """
    This is the loop that is run in a worker thread. It collects job from the
    job queue, execute it, and puts the result in the results queue. If the job
    is None, that means that all the jobs are done and that the thread can
    exit. If an error occurs while running the computation, we set the global
    exception flag, then re-raise the error to exit the thread.
    """
    global JOB_QUEUE
    global RESULTS_QUEUE
    while True:
        if get_err_flag():
            break
        job = get_from_queue(JOB_QUEUE)
        if job is None:
            JOB_QUEUE.task_done()
            return
        try:
            res = _do_job(job)
            if RESULTS_QUEUE is not None:
                put_in_queue(RESULTS_QUEUE, res)
            JOB_QUEUE.task_done()
        except IDontCareException as e:
            # depending on the type of exception, you might just want to
            # ignore it...
            JOB_QUEUE.task_done()
        except Exception as e:
            # ...or raise.
            JOB_QUEUE.task_done()
            raise_err_flag()
            raise e


def start_threads(more_args):
    global THREADS
    assert len(THREADS) == 0
    for w in range(WORKERS):
        t = threading.Thread(
            target=job_loop,
            # arguments that can be passed to parametrize the job loop. These
            # are not job specific arguments.
            args=(JOB_QUEUE, RESULTS_QUEUE, more_args))
        # t.daemon = True  # Not necessary, depends on your use case
        THREADS.append(t)
        t.start()


if __name__ == "__main__":
    start_threads()
    for job in job_generator():
        put_in_queue(JOB_QUEUE, job)
        if get_err_flag():
            break
    for _ in range(WORKERS):
        # Add a None for each worker to individually tell them to exit
        put_in_queue(JOB_QUEUE, None)

    #  Wait for all the thread to exit
    for t in THREADS:
        t.join()
    JOB_QUEUE.join()

    # Do something with the results
