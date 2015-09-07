#! /usr/bin/env python
# coding=utf-8

import os
import os.path
import sys
import time
import shutil
import socket
from multiprocessing import Process, Queue
import couchdb



class Task:
    def __init__(self, name, output_dir, log):
        self.__args = args
        sub_cls = getattr(__import__(name, fromlist=['SubTask']), 'SubTask')
        self.__sub_task = sub_cls(output_dir, log)
        self.__proc = None


    def is_initialized(self):
        return self.__sub_task.is_initialized()


    def initialize(self):
        self.__sub_task.initialize()


    def is_alive(self):
        return self.__proc is not None and self.__proc.is_alive()


    def run(self, output_dir, log):
        self.__q = Queue()
        self.__proc = Process(target=self.__sub_task.run args=(self.__q, self.__args))
        self.__proc.start()


    def is_run(self):
        return self.__proc is not None


    def collect_argrefs(self, tasks):
        pass # update self.__args


    def get_result(self):
        return {}


    def log_buf(self):
        return ""


class Req:
    def __init__(self, idx, tasks):
        self.__idx = idx
        self.__tasks = []
        self.__name2task = {}
        self.__running_task = None
        self.__finished = False

        global script_path

        for task in tasks:
            output_dir = os.path.join(script_path, 'results', doc['_id'], task_name)
            os.makedirs(output_dir) if not os.path.isdir(output_dir)
            task_log = output_dir + '.log'
            open(task_log, 'w').close() # recreate log for a task
            T = Task(task['name'], task['args'], output_dir, task_log)
            self.__tasks.append(T)
            self.__name2task[task['name']] = T


    def probe(self, doc, db):
        # prepare correct currently running task
        if self.__running_task is None:
            if len(self.__tasks) == 0:
                return
            else:
                self.__running_task = self.__tasks[0]
                del self.__tasks[0]

        if not self.__running_task.is_initialized():
            self.__running_task.initialize()
            return

        if self.__running_task.is_alive():
            db.put_attachment(doc, self.__running_task.log_buf(), 'log')
        elif self.__running_task.is_run(): # task is finished
            self.__dump() # TODO
            self.__running_task = None
        else: # start & init task
            if not self.__running_task.is_enabled():
                self.__running_task = None
            else:
                self.__running_task.collect_argrefs()
                self.__running_task.run()


    def finished(self):
        return len(self.__tasks) == 0


    def kill(self):
        if self.__running_task.is_alive():
            pass # TODO


    def __dump_results(self):
                    current_process.join()
                    with open(current_log) as f:
                        data = f.read()
                    full_log = os.path.join(workdir, 'full.log')
                    with open(full_log, 'a') as f:
                        f.write(data)
                    response = current_q.get()
                    doc['tasks'][current_task_name]['result'] = response['result']
                    doc['status'] = 'Preparing' if response['retcode'] == 0 else 'Failed'
                    db.save(doc)
        pass
                print " -", doc['status'], str(idx)
                with open(full_log) as f:
                    data = f.read()
                db.save(doc)
                db.put_attachment(doc, data, 'log')


def __idx_lock(doc, db):
    doc['status'] = 'Processed'
    doc['host'] = socket.gethostname()
    db.save(doc)


def __idx_locked(doc):
    return doc['status'] != 'Waiting'


def go():

    req = None

    while True:
        time.sleep(1)

        couch = couchdb.Server('http://10.1.0.35:5984')
        db = couch['patches']

        # process already started request
        if req is not None:
            try:
                doc = db[req.idx()]
            except couchdb.http.ResourceNotFound:
                continue # TODO

            if req.fihished(): # TODO dump overall status
                req = None
            elif doc['status'] == 'Kill':
                req.kill()
                req = None
            else:
                req.probe(doc, db)

        else: # select first non-locked request
            for idx in db:
                doc = db[idx] # TODO exception
                if not __idx_locked(doc):
                    req = Req(idx, doc['tasks'])
                    __idx_lock(doc, db)
                    break


if __name__ == '__main__':

    script_path = os.path.dirname(os.path.realpath(__file__))
    sys.path.append(os.path.join(script_path, 'tasks'))

    try:
        go()
    except KeyboardInterrupt:
        pass

