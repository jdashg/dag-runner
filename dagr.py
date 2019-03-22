#! /usr/bin/env python3

assert __name__ == '__main__'

####################
# dagr pseudo-module

class dagr(object):
    class Node(object):
        def __init__(self, deps, cmd=[]):
            for x in deps:
                assert(x)
            self.deps = set(deps)
            self.cmd = cmd


        def __str__(self):
            return '<>'.format(self.cmd)


        def then(self, cmd):
            return dagr.Node([self], cmd)


    class Target(Node):
        BY_NAME = dict()

        def __init__(self, name, *a, **k):
            super().__init__(*a, **k)
            self.name = name

            assert self.name not in dagr.Target.BY_NAME
            dagr.Target.BY_NAME[self.name] = self


        def __str__(self):
            return '{}{}'.format(self.name, super().__str__())


    @staticmethod
    def include(path, base_path=None):
        if not base_path:
            base_path = pathlib.Path(__file__).parent
        path = base_path / path
        try:
            data = path.read_bytes()
        except IOError:
            path = path / '.dagr'
            data = path.read_bytes()
        code = compile(data, str(path), 'exec')
        g = globals()
        was = g['__file__']
        g['__file__'] = path.as_posix()
        exec(code, g)
        g['__file__'] = was

####################
# Traverse from the root file

import pathlib
dagr.include('.dagr', pathlib.Path.cwd())

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
    visited = dict()
    def recurse(cur):
        print('cur', str(cur))
        if cur in stack:
            raise ExStackHasCycle(stack + [cur])
        try:
            return visited[cur]
        except KeyError:
            pass

        nexts = fn_nexts(cur)

        stack.append(cur)
        mapped_nexts = [recurse(x) for x in nexts]
        stack.pop()

        ret = fn_map(cur, mapped_nexts)
        visited[cur] = ret
        return ret

    return [recurse(x) for x in roots]

# -

NUM_THREADS = os.cpu_count()
KEEP_GOING = False
VERBOSE = False
DRY_RUN = False

def run_node(cur):
    is_shell = type(cur.cmd) is str

    if VERBOSE:
        sys.stderr.write(str(cur))

    if DRY_RUN:
        return

    p = subprocess.run(cur.cmd, shell=is_shell, capture_output=True, check=True)
    sys.stdout.buffer.write(p.stdout)
    sys.stderr.buffer.write(p.stderr)
    return

# -

def run_dag(roots):
    return_when = futures.FIRST_EXCEPTION
    if KEEP_GOING:
        return_when = futures.ALL_COMPLETED

    with futures.ThreadPoolExecutor(NUM_THREADS, 'dagr') as pool:
        def fn_nexts(cur):
            print(cur.cmd)
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

    if cur == '--':
        break
    if cur.startswith('-j'):
        NUM_THREADS = int(cur[2:])
        continue
    if cur == '-v':
        VERBOSE = True
        continue
    if cur == '--dry':
        DRY_RUN = True
        continue

    args.insert(0, cur)
    break

# -

root_names = args
if not root_names:
    root_names = ['DEFAULT']
print('root_names', root_names)

roots = []
for x in root_names:
    try:
        roots.append(dagr.Target.BY_NAME[x])
    except KeyError:
        sys.stderr.write('No such node: {}\n'.format(x))
        exit(1)

print('roots', roots)
# -

try:
    run_dag(roots)
except Exception as e:
    sys.stdout.write('BUILD FAILED:\n\n\n')
    raise e

elapsed_time = time.time() - start_time
sys.stdout.write('BUILD SUCCEEDED (in {:.4}s)\n'.format(elapsed_time))
