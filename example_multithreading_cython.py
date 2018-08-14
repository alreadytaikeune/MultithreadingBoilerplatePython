#! encoding=utf8
from __future__ import unicode_literals, print_function, absolute_import
from time import time
from boilerplate_multithreading_class import Executor
from example_functions import syracuse, levenstein


class SyracuseExecutor(Executor):

    def _do_job(self, job, *args):
        s = time()
        res = syracuse(int(job))
        print("job took {}".format(time() - s))
        return res

    def prepare_result(self, job, result):
        return (job, result)


class LevensteinExecutor(Executor):

    def prepare_job_args(self, job, name):
        return (job, name)

    def _do_job(self, job, name, *args):
        st = time()
        res = levenstein(name, job)
        # print("{}".format(time()-st))
        return res

    def prepare_result(self, job, result, name):
        return (name, job, result)


if __name__ == "__main__":
    import random

    def seq():
        return r''.join(
                [random.choice(['A', 'T', 'C', 'G']) for i in range(2000)])

    def gen(size):
        for _ in range(size):
            yield seq()
    s = seq()
    for w in range(3, -1, -1):
        print("{} WORKERS".format(w+1))
        ex = LevensteinExecutor(1000, w+1, threads_args=[s],
                                max_results_queue_size=-1)
        st = time()
        ex.execute(list(gen(500)))
        print("Executed in {:.2f} seconds".format(time()-st))
        # print(min(ex.results_queue.queue, key=lambda x: x[2]))
