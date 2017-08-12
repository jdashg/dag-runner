#!/usr/bin/env python2.7

import multiprocessing
import subprocess
import sys
import threading
import time
import traceback


LIMP_AFTER_FAILURE = False


log_lock = threading.Lock()
def log(text):
    with log_lock:
        print text

####################
# Helpers

class DagrPattern(object):
    def __init__(self, args):
        self.args = args


    def apply(self, suffix):
        return self.args + suffix


    def map(self, from_list):
        return map(lambda x: self.apply([x]), from_list)

####################
# Node

class DepNode(object):
    def __init__(self, parents):
        self._parents = set(parents)
        self._children = set()
        for parent in self._parents:
            parent._children.add(self)


class DagrNode(DepNode):
    nodes_by_name = dict()


    def __init__(self, name, parents=[], cmd=None):
        DepNode.__init__(self, parents)
        self._name = name

        assert name not in DagrNode.nodes_by_name
        DagrNode.nodes_by_name[name] = self

        self.cmds = []
        if cmd:
            self.cmds.append(cmd)
        return


    def run(self, resolve_func):
        if not self.cmds:
            resolve_func(True)
            return
        p = DagrNode.Progress(self, resolve_func)
        for i in range(len(self.cmds)):
            POOL.enqueue( Task(p.run, args=(i,)) )


    class Progress(object):
        def __init__(self, node, resolve_func):
            self.node = node
            self.lock = threading.Lock()
            self.pending_cmds = len(node.cmds)
            self.resolve_func = resolve_func


        def run(self, i):
            success = self.node.run_cmd(i)
            with self.lock:
                if success:
                    self.pending_cmds -= 1
                    if self.pending_cmds:
                        return
                if self.resolve_func:
                    self.resolve_func(success)
                    self.resolve_func = None


    def run_cmd(self, i):
        cmd = self.cmds[i]
        success = False
        try:
            subprocess.check_call(cmd)
            success = True
        except OSError:
            log('Binary not found: ' + cmd[0])
        except subprocess.CalledProcessError:
            log('Error running: {}'.format(cmd))
        return success


####################
# Traverse!

execfile('.dagr')

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

        for i in range(num_threads):
            t_name = '{}[{}]'.format(self.name, i)
            t = threading.Thread(name=t_name, target=self.thread, args=(i,))
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

####################

class NodeStatus(object):
    def __init__(self, graph, node):
        self.graph = graph
        self.node = node
        self.lock = threading.Condition()
        self.failed = False
        return


    def populate_parents_and_children(self, status_by_node):
        self.parents = set(map(lambda x: status_by_node[x], self.node._parents))
        self.children = set()
        for x in self.node._children:
            try:
                self.children.add(status_by_node[x])
            except KeyError:
                continue
        self.pending_parents = set(self.parents)


    def set_parents_for_root(self, parents):
        self.parents = set(parents)
        for x in self.parents:
            x.children.add(self)
        self.children = set()
        self.pending_parents = set(self.parents)


    def begin(self):
        assert not self.pending_parents

        if not self.node: # Root node
            self.graph.resolve_root(not self.failed)
            return

        if self.failed:
            self.end(False)
            return

        self.node.run(self.end)


    def end(self, success):
        if not success:
            self.failed = True
        for x in self.children:
            x.on_parent_resolved(self)


    def on_parent_resolved(self, parent):
        if parent.failed:
            self.failed = True

        with self.lock:
            self.pending_parents.remove(parent)
            if self.pending_parents:
                return
        self.graph.enqueue( Task(self.begin) )


def gather_leaves(cur, visited_set, stack, leaf_set):
    if cur in visited_set:
        if cur in stack:
            cycle = stack[stack.index(cur):] + [cur]
            cycle_names = map(lambda x: x._name, cycle)
            log('Cycle detected: {}'.format('->'.join(cycle_names)))
            exit(1)
        return

    visited_set.add(cur)

    if not cur._parents:
        leaf_set.add(cur)
        return

    stack.append(cur)
    for x in cur._parents:
        gather_leaves(x, visited_set, stack, leaf_set)

    stack.pop()
    return


class Future(object):
    def __init__(self):
        self.ready = threading.Event()

    def set(self, value):
        self.value = value
        self.ready.set()

    def get(self):
        self.ready.wait()
        return self.value


class DepGraphWalker(object):
    def __init__(self, roots):
        self.task_queue = ThreadPool('task queue', 1)
        self.future = Future()

        leaves = set()
        visited_set = set()
        for x in roots:
            gather_leaves(x, visited_set, [], leaves)
        assert leaves

        self.status_by_node = dict()
        for x in visited_set:
            self.status_by_node[x] = NodeStatus(self, x)

        for x in self.status_by_node.itervalues():
            x.populate_parents_and_children(self.status_by_node)

        self.root = NodeStatus(self, None)
        root_statuses = map(lambda x: self.status_by_node[x], roots)
        self.root.set_parents_for_root(root_statuses)

        for x in leaves:
            status = self.status_by_node[x]
            self.enqueue( Task(status.begin) )


    def enqueue(self, task):
        self.task_queue.enqueue(task)

    def resolve_root(self, success):
        self.future.set(success)

    def wait(self):
        ret = self.future.get()
        self.task_queue.kill()
        return ret


POOL = ThreadPool('POOL', multiprocessing.cpu_count())


if __name__ == '__main__':
    root_names = ['DEFAULT']
    if len(sys.argv) > 1:
        root_names = sys.argv[1:].split(' ')

    roots = []
    for x in root_names:
        try:
            roots.append(DagrNode.nodes_by_name[x])
        except KeyError:
            print >>sys.stderr, 'No such node: {}'.format(x)
            exit(1)

    walker = DepGraphWalker(roots)
    success = walker.wait()
    POOL.kill()

    #print threading.enumerate()

    exit(int(not success))
