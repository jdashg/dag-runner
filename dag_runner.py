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

class DagrPattern:
    def __init__(self, args):
        self.args = args
        return

    def apply(self, suffix):
        return self.args + suffix

    def map(self, from_list):
        return map(lambda x: self.apply([x]), from_list)

####################
# Node

class DagrNode:
    nodes_by_name = dict()

    class ExFailed(Exception):
        pass


    def __init__(self, name, parents=[], cmd=None):
        self._name = name
        self._parents = parents
        self.cmds = []
        if cmd:
            self.cmds.append(cmd)

        self._is_done = threading.Event()
        self._failed = False

        assert name not in DagrNode.nodes_by_name
        DagrNode.nodes_by_name[name] = self
        return


    def accum(self, accum_set):
        if self in accum_set:
            return
        accum_set.add(self)
        for x in self._parents:
            x.accum(accum_set)
        return


    def wait(self):
        self._is_done.wait()
        return not self._failed


    def thread(self, engine):
        for x in self._parents:
            #log('{} waiting on {}'.format(self._name, x._name))
            if not x.wait():
                self._failed = True
                self._is_done.set()
                return

        #log('done with parents for {}'.format(self._name))
        self.sem = threading.Semaphore(1)

        engine.enqueue(self, len(self.cmds))

        for x in self.cmds:
            self.sem.acquire()
            if self._failed:
                break;
        self._is_done.set()
        return


    def process(self, i):
        cmd = self.cmds[i]
        try:
            subprocess.check_call(cmd)
        except OSError:
            log('Binary not found: ' + cmd[0])
            self._failed = True
        except subprocess.CalledProcessError:
            log('Error running: {}'.format(cmd))
            self._failed = True
        self.sem.release()
        return self._failed


####################
# Traverse!

execfile('.dagr')

####################
# Privates

class DagrEngine:
    def __init__(self, max_threads):
        self.cond = threading.Condition()
        self.pending = []
        self.active_threads = 0
        self.starve_timer = None
        self.is_done = False

        for i in range(max_threads):
            t = threading.Thread(target=self.slot_thread)
            #t.daemon = True
            t.start()

        return

    def on_thread_activate(self):
        self.active_threads += 1
        if self.starve_timer:
            self.starve_timer.cancel()
            self.starve_timer = None

    def on_thread_deactivate(self):
        self.active_threads -= 1
        if self.is_done:
            return
        if not self.starve_timer:
            self.starve_timer = threading.Timer(0.1, self.on_starved)
            self.starve_timer.start()


    def slot_thread(self):
        with self.cond:
            self.on_thread_activate()

        while not self.is_done:
            with self.cond:
                if not self.pending:
                    self.on_thread_deactivate()
                    self.cond.wait()
                    self.on_thread_activate()
                    continue

                (node, i) = self.pending.pop(0)

            if node.process(i):
                continue
            self.kill()
            break
        with self.cond:
            self.on_thread_deactivate()
        return


    def enqueue(self, node, cmd_count):
        with self.cond:
            for i in range(cmd_count):
                self.pending.append((node, i))
            self.cond.notify_all()
        return


    def run(self, roots):
        nodes = set()
        for x in roots:
            x.accum(nodes)
        #log(str(map(lambda x: x._name, nodes)))

        for x in nodes:
            t = threading.Thread(target=x.thread, args=(self,))
            t.start()

        failed = False
        for x in roots:
            if not x.wait():
                failed = True
                break

        self.kill()
        return not failed

    def kill(self):
        log('Killing...')
        with self.cond:
            self.is_done = True
            if self.starve_timer:
                self.starve_timer.cancel()
            self.cond.notify_all()


    def on_starved(self):
        log('Starved waiting for work. Killing...')
        self.kill()


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


    engine = DagrEngine(multiprocessing.cpu_count())
    engine.run(roots)

    #log('DONE')
    exit(0)
