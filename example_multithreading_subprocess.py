#! encoding=utf8
from __future__ import unicode_literals, print_function, absolute_import
from subprocess import Popen, PIPE

from boilerplate_multithreading_class import Executor


class SubprocessExecutor(Executor):

    def _do_job(self, job, script_name, *args):
        if not isinstance(job, list):
            job = [job]
        all_args = [script_name] + job + list(args)
        all_args = [str(a) for a in all_args]
        p = Popen(all_args, stdout=PIPE, stderr=PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise RuntimeError(
                "Got error {} while running script {}: {}".format(
                    p.returncode, script_name, stderr))
        return stdout.strip()


class HashCashSubprocess(SubprocessExecutor):

    def prepare_job_args(self, job, script_name, difficulty):
        return (job, script_name, difficulty)

    def prepare_result(self, job, result, script, *args):
        args = list(args)
        return tuple([job] + args + [result])


if __name__ == "__main__":
    import string
    import random
    import os
    import time

    def get_id(size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    def gen_id(size):
        for _ in range(size):
            yield get_id()

    for w in range(4):
        print("{} WORKERS".format(w+1))
        ex = HashCashSubprocess(
            20, w+1, threads_args=[os.path.abspath("hashcash.py"), 8],
            max_results_queue_size=-1)
        st = time.time()
        print(ex.execute(gen_id(100)))
        end = time.time()
        print("Executed in {:.2f} seconds".format(end-st))
