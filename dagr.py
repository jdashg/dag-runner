#!/usr/bin/env python3

####################
# Node

class DagrNode(object):
    nodes_by_name = dict()

    def __init__(self, name, parents=[], cmd=None):
        self.name = name
        self.parents = set(parents)

        assert name not in DagrNode.nodes_by_name
        DagrNode.nodes_by_name[name] = self

        self.cmds = []
        if cmd:
            self.cmds.append(cmd)
        return

####################
# Traverse!

DAGR_FILENAME = '.dagr'
with open(DAGR_FILENAME) as f:
    code = compile(f.read(), DAGR_FILENAME, 'exec')
    exec(code)

####################
# Now include

import multiprocessing
import shlex
import subprocess
import sys
import threading
import time

####################
# Privates

class Task(object):
    def __init__(self, target, args=(), kwargs={}):
        self.target = target
        self.args = args
        self.kwargs = kwargs

    def run(self):
        self.target(*self.args, **self.kwargs)


class ThreadPool(object):
    def __init__(self, name, num_threads):
        self.name = name
        self.cond = threading.Condition()
        self.is_alive = True
        self.task_queue = []

        self.threads = set()
        for i in range(num_threads):
            t_name = '{}[{}]'.format(self.name, i)
            t = threading.Thread(name=t_name, target=self.thread, args=(i,))
            self.threads.add(t)
            t.start()


    def enqueue(self, task):
        assert self.is_alive
        with self.cond:
            self.task_queue.append(task)
            self.cond.notify_all()


    def thread(self, thread_i):
        while self.is_alive:
            with self.cond:
                try:
                    task = self.task_queue.pop(0)
                except IndexError:
                    self.cond.wait()
                    continue

            task.run()


    def kill(self):
        self.is_alive = False
        with self.cond:
            self.cond.notify_all()

    def join(self):
        for t in self.threads:
            t.join()

####################

class ExDagHasCycle(Exception):
    def __init__(self, stack, cycle):
        self.stack = stack
        self.cycle = cycle


def map_dag(roots, map_func):
    mapped_nodes = dict()
    stack = []

    def recurse(node):
        if node in stack:
            cycle = stack[stack.index(cur):] + [cur]
            raise ExDagHasCycle(stack, cycle)

        try:
            mapped = mapped_nodes[node]
            return mapped
        except KeyError:
            pass

        # --

        stack.append(node)

        parents = node.parents
        mapped_parents = set()
        for parent in parents:
            mapped_parent = recurse(parent)
            if mapped_parent:
                mapped_parents.add(mapped_parent)

        stack.pop()

        # --

        mapped = map_func(node, mapped_parents, stack)

        mapped_nodes[node] = mapped
        return mapped


    mapped_roots = set()
    for x in roots:
        mapped = recurse(x)
        if mapped:
            mapped_roots.add(mapped)

    return mapped_roots

####################

class PromiseGraphNode(object):
    def __init__(self, parents, info=''):
        self.info = str(info)
        #print 'PromiseGraphNode {}: '.format(self.info) + ', '.join(map(lambda x: x.info, parents))
        self.lock = threading.Lock()
        self.pending_parents = set(parents)
        self.children = set()
        for parent in self.pending_parents:
            with parent.lock:
                parent.children.add(self)
        self.result = None

    def run(self):
        self.resolve(True)

    def on_resolve(self):
        pass

    def resolve(self, result):
        assert result in (True, False)
        with self.lock:
            if self.result != None:
                return
            self.result = result
            self.pending_parents.clear()

        self.on_resolve()

        assert self.result in (True, False)
        if not self.result:
            for child in self.children:
                child.resolve(False)
        else:
            for child in self.children:
                with child.lock:
                    try:
                        child.pending_parents.remove(self)
                    except KeyError:
                        continue
                    if child.pending_parents:
                        continue
                assert child.result == None
                child.run()

####################

def quote_args(args):
    if os.name == 'nt':
        return subprocess.list2cmdline(args)
    return shlex.quote(args)


class SubprocCallNode(PromiseGraphNode):
    ECHO = False
    def __init__(self, parents, info, pool, call):
        PromiseGraphNode.__init__(self, parents, info)
        self.pool = pool
        self.call = call

    def run(self):
        self.pool.enqueue(Task(self.task_run))

    def task_run(self):
        shell = type(self.call) is str

        if SubprocCallNode.ECHO:
            call = self.call
            if not shell:
                call = quote_args(call)
            sys.stdout.write(call + '\n')
            self.resolve(True)
            return

        result = False
        try:
            p = subprocess.Popen(self.call, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (stdout, stderr) = p.communicate()
            stdout = stdout.decode(errors='replace')
            stderr = stderr.decode(errors='replace')

            if p.returncode == 0:
                result = True
        except OSError:
            stdout = ''
            stderr = 'Binary not found: {}'.format(self.call[0])

        if not result:
            stderr += '\n{} FAILED: {}\n'.format(self.info, self.call)
        sys.stdout.write(stdout)
        sys.stderr.write(stderr)

        self.resolve(result)

####################

class EventNode(PromiseGraphNode):
    def __init__(self, parents, info=''):
        PromiseGraphNode.__init__(self, parents, info)
        self.event = threading.Event()

    def on_resolve(self):
        self.event.set()

####################

NUM_THREADS = multiprocessing.cpu_count()
#NUM_THREADS = 1

def run_dagr(roots, thread_count=NUM_THREADS):
    pool = ThreadPool('run_dagr pool', thread_count)

    mapped_leaves = set()
    def map_dagr_node(node, mapped_parents, stack):
        if not node.cmds:
            begin = PromiseGraphNode(mapped_parents, node.name)
            end = begin
        elif len(node.cmds) == 1:
            begin = SubprocCallNode(mapped_parents, node.name, pool, node.cmds[0])
            end = begin
        else:
            begin = PromiseGraphNode(mapped_parents, node.name + '.begin')
            subs = set()
            for i, cmd in enumerate(node.cmds):
                info = node.name + '.{}'.format(i)
                sub = SubprocCallNode([begin], info, pool, cmd)
                subs.add(sub)
            end = PromiseGraphNode(subs, node.name + '.end')

        if not mapped_parents:
            mapped_leaves.add(begin)

        return end

    mapped_roots = map_dag(roots, map_dagr_node)

    # --

    terminal_root = EventNode(mapped_roots, '<terminal_root>')

    for x in mapped_leaves:
        x.run()

    terminal_root.event.wait()
    pool.kill()
    pool.join()
    return terminal_root.result


if __name__ == '__main__':
    start_time = time.time()
    args = sys.argv[1:]
    while True:
        try:
            cur = args.pop(0)
        except IndexError:
            break

        if cur == '--dump':
            SubprocCallNode.ECHO = True
            continue
        if cur == '--':
            break
        args.insert(0, cur)
        break

    root_names = args
    if not root_names:
        root_names = ['DEFAULT']

    roots = []
    for x in root_names:
        try:
            roots.append(DagrNode.nodes_by_name[x])
        except KeyError:
            sys.stderr.write('No such DagrNode: {}\n'.format(x))
            exit(1)

    success = run_dagr(roots)

    elapsed_time = time.time() - start_time
    if success:
        sys.stderr.write('BUILD SUCCEEDED (in {:.4}s)\n'.format(elapsed_time))
    else:
        sys.stderr.write('BUILD FAILED\n\n\n')

    assert len(threading.enumerate()) == 1, str(threading.enumerate())
    exit(int(not success))
