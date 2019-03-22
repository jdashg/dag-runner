#! /usr/bin/env python3

assert __name__ == '__main__'

####################
# dagr pseudo-module

class dagr(object):
    VERBOSE = False
    DRY_RUN = False

    class Node(object):
        def __init__(self, deps, cmd=[]):
            import os
            for x in deps:
                assert(x)
            self.deps = set(deps)
            self.cmd = cmd
            self.cwd = os.getcwd()


        def __str__(self):
            return f'<{self.cmd}>'


        def then(self, cmd):
            return dagr.Node([self], cmd)


        def run(self):
            is_shell = type(self.cmd) is str

            if dagr.VERBOSE:
                sys.stderr.write(f'{self}\n')

            if dagr.DRY_RUN:
                if self.cmd:
                    if is_shell:
                        line = self.cmd
                    else:
                        line = ' '.join((shlex.quote(x) for x in self.cmd))
                    sys.stdout.write(f'{line}\n')
                return

            if self.cmd:
                p = subprocess.run(self.cmd, shell=is_shell, cwd=self.cwd, capture_output=True)
                sys.stdout.buffer.write(p.stdout)
                sys.stderr.buffer.write(p.stderr)
                if p.returncode:
                    raise Exception(f'Cmd `{self.cmd}` failed.')
            return


    class Target(Node):
        BY_NAME = dict()

        def __init__(self, name, *a, **k):
            super().__init__(*a, **k)
            self.name = name

            assert self.name not in dagr.Target.BY_NAME
            dagr.Target.BY_NAME[self.name] = self


        def __str__(self):
            return f'{self.name}{super().__str__()}'


    @staticmethod
    def include(path):
        import os
        path = pathlib.Path(path)
        if path.is_dir():
            path = path / '.dagr'

        data = path.read_bytes()

        code = compile(data, str(path), 'exec')

        pushd = pathlib.Path.cwd()
        os.chdir(path.parent)
        exec(code, globals())
        os.chdir(pushd)

####################
# Traverse from the root file

import pathlib
dagr.include('.dagr')

####################
# Now include

from concurrent import futures
import os
import shlex
import subprocess
import sys
import time

py_version = sys.version_info[:2]
# Require subprocess.run(capture_output=)
assert py_version >= (3, 7), py_version

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

KEEP_GOING = False
NUM_THREADS = os.cpu_count()

# -

def run_dag(roots):
    return_when = futures.FIRST_EXCEPTION
    if KEEP_GOING:
        return_when = futures.ALL_COMPLETED

    def wait_for(fs):
        (done, not_done) = futures.wait(fs, return_when=return_when)
        if not KEEP_GOING:
            for f in not_done:
                f.cancel()
        for f in done:
            e = f.exception()
            if e:
                raise e

    def wait_and_run(cur, mapped_nexts):
        #print(f'wait_and_run({cur})')
        wait_for(mapped_nexts)
        cur.run()

    with futures.ThreadPoolExecutor(NUM_THREADS, 'dagr') as pool:

        def fn_nexts(cur):
            #print(f'fn_nexts({cur})')
            return cur.deps

        def fn_map(cur, mapped_nexts):
            #print(f'fn_map({cur}, {mapped_nexts})')
            return pool.submit(wait_and_run, cur, mapped_nexts)

        root_futures = MapDag(roots, fn_nexts, fn_map)
        wait_for(root_futures)

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
        dagr.VERBOSE = True
        continue
    if cur == '--dry':
        dagr.DRY_RUN = True
        continue

    args.insert(0, cur)
    break

# -

root_names = args
if not root_names:
    root_names = ['DEFAULT']
sys.stderr.write(f'Building: {root_names}\n')

roots = []
for x in root_names:
    try:
        roots.append(dagr.Target.BY_NAME[x])
    except KeyError:
        sys.stderr.write('No such node: {}\n'.format(x))
        exit(1)

# -

try:
    run_dag(roots)
except Exception as e:
    sys.stderr.flush()
    sys.stdout.flush()
    sys.stdout.write('\nBUILD FAILED:\n')
    raise e

elapsed_time = time.time() - start_time
sys.stderr.flush()
sys.stdout.flush()
sys.stderr.write('\nBUILD SUCCEEDED ({:.3f}s)\n'.format(elapsed_time))
