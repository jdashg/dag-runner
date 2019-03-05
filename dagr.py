#! /usr/bin/env python3

assert __name__ == '__main__'

####################
# dagr pseudo-module

class dagr(object):
    class Node(object):
        BY_NAME = dict()

        def __init__(self, deps, cmd=[], name=None):
            self.deps = set(deps)
            self.cmd = cmd
            self.name = name

            if self.name:
                assert self.name not in Node.BY_NAME
                Node.BY_NAME[self.name] = self


        def __str__(self):
            ret = '<>'.format(self.cmd)
            if self.name:
                ret = '{} {}'.format(self.name, ret)
            return ret


        def then(self, cmd):
            return Node([self], cmd)


    @staticmethod
    def include(path):
        with open(path) as f:
            code = compile(f.read(), path, 'exec')
            exec(code)

####################
# Traverse from the root file

dagr.include('.dagr')

####################
# Now include

from concurrent import futures
import os
import subprocess
import sys
import time

####################
# Privates

class ExStackHasCycle(Exception):
    def __init__(self, stack):
        prev_cur = stack.index(stack[-1])
        self.stack = stack[0:prev_cur]
        self.cycle = stack[prev_cur:]


def MapDag(roots, fn_nexts, fn_map):
    stack = []
    visited = set()
    def recurse(cur):
        if cur in stack:
            raise ExStackHasCycle(stack + [cur])
        if cur in visited:
            continue
        visited.add(cur)

        nexts = fn_nexts(cur)

        stack.append(cur)
        mapped_nexts = [recurse(x) for x in nexts]
        stack.pop()

        return fn_map(cur, mapped_nexts)

    return [recurse(x) for x in roots]

# -

NUM_THREADS = os.cpu_count()
#NUM_THREADS = 1

RETURN_WHEN = futures.ALL_COMPLETED
ECHO = False

# -

def run_node(cur):
    is_shell = type(cur.cmd) is str

    if ECHO:
        sys.stderr.write(str(cur))
        return

    p = subprocess.run(cur.cmd, shell=is_shell, capture_output=True, check=True)
    sys.stdout.buffer.write(p.stdout)
    sys.stderr.buffer.write(p.stderr)
    return

# -

def run_dag(roots, thread_count=NUM_THREADS, return_when=RETURN_WHEN):
    with futures.ThreadPoolExecutor(thread_count, 'dagr') as pool:
        def fn_nexts(cur):
            return cur.deps

        def wait_and_run(cur, mapped_nexts):
            futures.wait(mapped_nexts, return_when=return_when)
            run_node(cur)

        def fn_map(cur, mapped_nexts):
            return pool.submit(wait_and_run, [cur, mapped_nexts])

        root_futures = MapDag(roots, fn_nexts, fn_map)
        futures.wait(root_futures, return_when=return_when)

# -

start_time = time.time()
args = sys.argv[1:]
while True:
    try:
        cur = args.pop(0)
    except IndexError:
        break

    if cur == '--dump':
        ECHO = True
        continue
    if cur == '--':
        break
    args.insert(0, cur)
    break

# -

root_names = args
if not root_names:
    root_names = ['DEFAULT']

roots = []
for x in root_names:
    try:
        roots.append(dagr.Node.BY_NAME[x])
    except KeyError:
        sys.stderr.write('No such node: {}\n'.format(x))
        exit(1)

# -

try:
    run_dagr(roots)
except e:
    sys.stdout.write('BUILD FAILED:\n\n\n')
    raise e

elapsed_time = time.time() - start_time
sys.stdout.write('BUILD SUCCEEDED (in {:.4}s)\n'.format(elapsed_time))
