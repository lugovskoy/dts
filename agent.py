#! /usr/bin/env python
# coding=utf-8

import os
import os.path
import sys
import time
import json
import copy
import shutil
import socket
import logging as logger
from multiprocessing import Process, Queue
import couchdb
import subprocess
import traceback
import pickle
import io
import marshal, types
import argparse
import importlib
#from contextlib import redirect_stdout


__COUCH_DB_SRV    = "localhost"
__COUCH_DB_REQ_T  = "dts_requests"
__COUCH_DB_CONF_T = "dts_config"


class Task:
    def __init__(self, name, opts, resdir):
        self.__name = name
        self.__opts = opts
        self.__resdir = resdir
        self.__proc = None
        self.__is_finished = False
        self.__results = None
        self.__refs = {}

        task_mod = __import__(name)

        installed_version = int(task_mod.Task.version)
        actual_version = int(self.__opts['version'])

        if installed_version < actual_version: # update installed task
            raise Exception("Version {0} for task {1} is too old, task must be updated to {2}".format(installed_version, name, actual_version))

        f = io.StringIO()
        #with redirect_stdout(f):
        self.__cls = task_mod.Task()
        #print('Got stdout: "{0}"'.format(f.getvalue()))


    def __collect_argrefs(self, name2task):
        if not hasattr(self.__cls, 'refs'):
            return dict()
        inrefs = self.__cls.refs
        if not isinstance(inrefs, dict):
            raise Exception("Refs are not dict")

        logger.debug('refs found: {0}'.format(inrefs))

        outrefs = {}
        for name, ref in inrefs.items():
            rtask_name, rtask_retval_name = ref.split('.')
            rtask_res = name2task[rtask_name].get_result()
            if rtask_retval_name not in rtask_res:
                raise Exception('Task {0} does not return val {1} referenced by another task'.format(rtask_name, rtask_retval_name))
            outrefs[name] = rtask_res[rtask_retval_name]
        return outrefs


    def run(self, name2task):
        refs = self.__collect_argrefs(name2task)
        self.__q = Queue()
        self.__proc = Process(target=self.__cls, args=(self.__opts['args'], refs, self.__resdir, self.__q))
        self.__proc.start()


    def is_alive(self):
        return self.__proc is not None and self.__proc.is_alive()


    def probe(self):
        if self.__proc is not None and not self.__proc.is_alive():
            self.__results = copy.deepcopy(self.__q.get())
            self.__proc.join()
            self.__proc = None
            self.__is_finished = True
    
    
    def is_finished(self):
        return self.__is_finished


    def get_result(self):
        return self.__results


    def get_name(self):
        return self.__name


    def get_log(self):
        return "xxx"


class Req:
    def __init__(self, idx, tasks):
        self.__idx = idx
        self.__tasks = []
        self.__name2task = {}
        self.__proc_task = None

        for task_name, task_opts in tasks.items():
            logger.debug('Adding new task {0} with args {1}'.format(task_name, task_opts['args']))
            
            resdir = os.path.join(script_path, 'results', self.__idx, task_name)
            if not os.path.isdir(resdir):
                os.makedirs(resdir)

            T = Task(task_name, task_opts, resdir)
            self.__tasks.append(T)
            self.__name2task[task_name] = T


    def probe(self, doc, db):
        # prepare correct currently running task
        if self.__proc_task is None:
            logger.debug('probing with {0} {1}'.format(self.__tasks, self.__proc_task))
            if len(self.__tasks) > 0:
                self.__proc_task = self.__tasks[0]
                self.__proc_task.run(self.__name2task)
                del self.__tasks[0]
        else:
            logger.debug('running task {0} is alive'.format(self.__proc_task))
            self.__proc_task.probe()
            if self.__proc_task.is_finished():
                doc['tasks'][self.__proc_task.get_name()]['result'] = self.__proc_task.get_result()
            task_log_buf = self.__proc_task.get_log()
            db.save(doc)
            db.put_attachment(doc, task_log_buf, 'log')
            if self.__proc_task.is_finished():
                self.__proc_task = None


    def is_finished(self):
        return self.__proc_task is None and len(self.__tasks) == 0


    def get_idx(self):
        return self.__idx


    def kill(self):
        pass # TODO


def __idx_lock(idx, db):
    doc = db[idx]
    if 'status' in doc and doc['status'] == 'Waiting':
        doc['status'] = 'Processed'
        doc['host'] = socket.gethostname()
        try:
            db[doc.id] = doc
        except couchdb.http.ResourceConflict:
            return False
        else:
            return True


def __idx_unlock(idx, db):
    doc = db[idx]
    doc['status'] = 'Failed'
    db[doc.id] = doc


def lock_db_table(db, table, timeout=0.1):
    is_locked = False
    while not is_locked:
        doc = db[table]
        while 'locked' in doc and doc['locked']:
            logger.debug('table "{0}" is locked -- waiting '.format(table))
            time.sleep(timeout)
            doc = db[table]
        doc['locked'] = True
        try:
            db[doc.id] = doc
        except couchdb.http.ResourceConflict:
            is_locked = False
        else:
            is_locked = True


def unlock_db_table(db, table):
    doc = db[table]
    if 'locked' not in doc or not doc['locked']:
        return
    is_unlocked = False
    while not is_unlocked:
        doc = db[table]
        doc['locked'] = False
        try:
            db[doc.id] = doc
        except couchdb.http.ResourceConflict:
            is_unlocked = False
        else:
            is_unlocked = True


def update_tasks(couch):
    global script_path
    if __COUCH_DB_CONF_T not in couch:
        couch.create(__COUCH_DB_CONF_T)
    db = couch[__COUCH_DB_CONF_T]

    if 'config' not in db:
        db['config'] = {'names': [], 'opts': {}}

    output = subprocess.check_output(['git', '--git-dir={0}'.format(os.path.join(script_path, '.git')), 'pull'])
    if 'Already up-to-date.' not in output:
        logger.debug('Scripts have been updates -- restarting agent...')
        os.execv(__file__, sys.argv)

    try:
        lock_db_table(db, 'config')

        logger.debug('config is locked')

        doc = db['config']
        conf_names = doc['names']
        conf_opts = doc['opts']

        # load task configs
        logger.debug('Looking up for task modules in ' + tasks_dir)

        tasks_to_update = []
        all_task_names = set(conf_names + os.listdir(tasks_dir))
        for task_name in all_task_names:
            logger.debug('Checking task {0}'.format(task_name))
            try:
                task_mod = importlib.import_module(task_name)
            except ImportError, e:
                tasks_to_update.append(task_name)
                logger.debug('Cannot import task {0}: {1}'.format(task_name, e))
                continue

            # TODO
            #if not all(hasattr(task_mod, a) for a in ['__version__', '__arguments', 'Task', 'setup']):
            #    raise Exception("Some of module {0} argument is missed {1}".format(task_name, [hasattr(task_mod, a) for a in ['__version__', '__arguments', 'Task', 'setup']]))

            setup_code = marshal.dumps(task_mod.Task.setup.func_code)
            setup_str = setup_code.encode('base64')
            task_opts = {'title': task_mod.Task.title,
                         'version': task_mod.Task.version,
                         'args': task_mod.Task.arguments,
                         'init': setup_str}
            if task_name not in conf_names:
                conf_names.append(task_name)
                conf_opts[task_name] = task_opts
                logger.debug('New task -> add to db')
            else:
                actual_version = int(conf_opts[task_name]['version'])
                installed_version = int(task_mod.Task.version)
                if installed_version < actual_version: # update installed task
                    logger.debug('Task should be updated')
                    tasks_to_update.append(task_name)
                elif installed_version > actual_version: # update db
                    conf_opts[task_name] = task_opts
                    logger.debug('Update task config')

        db[doc.id] = doc
        unlock_db_table(db, 'config')

    except KeyboardInterrupt:
        unlock_db_table(db, 'config')
        sys.exit(1)

    logger.debug('config is unlocked')

    for task_name in tasks_to_update:
        logger.debug('Updating task {0}'.format(task_name))
        setup_str = conf_opts[task_name]['init']
        setup_code = setup_str.decode('base64')
        code = marshal.loads(setup_code)
        setup_fun = types.FunctionType(code, globals(), 'setup')
        task_dir = os.path.join(tasks_dir, task_name)
        if os.path.isdir(task_dir):
            shutil.rmtree(task_dir)
        os.makedirs(task_dir)
        setup_fun(task_dir)


def go():
    req = None

    while True:
        time.sleep(1)

        couch = couchdb.Server(__COUCH_DB_SRV)
        if __COUCH_DB_REQ_T not in couch:
            continue
        db = couch[__COUCH_DB_REQ_T]

        logger.debug('new step')

        # process already started request
        if req is not None:
            try:
                doc = db[req.get_idx()]
            except couchdb.http.ResourceNotFound:
                logger.debug('Couchdb is anavailable')
                return

            req.probe(doc, db)

            if req.is_finished(): # TODO dump overall status
                logger.debug('req is finished')
                #__idx_unlock(doc, db)
                req = None
            elif doc['status'] == 'Kill':
                req.kill()
                req = None

        else: # select first non-locked request
            try:
                update_tasks(couch)
            except Exception, e:
                print(traceback.format_exc())
                logger.warning('Cannot update tasks because of {0}'.format(e))
                return

            for idx in db:
                if __idx_lock(idx, db):
                    doc = db[idx]
                    try:
                        req = Req(idx, doc['tasks'])
                    except Exception, e:
                        __idx_unlock(idx, db)
                        print(traceback.format_exc())
                        logger.warning('Cannot create new reqest because of {0}'.format(e))
                        req = None
                    break


if __name__ == '__main__':
    logger.basicConfig(level=logger.DEBUG)

    script_path = os.path.dirname(os.path.realpath(__file__))
    tasks_dir = os.path.join(script_path, 'tasks')
    if not os.path.isdir(tasks_dir):
        os.makedirs(tasks_dir)
    sys.path.append(os.path.join(script_path, 'tasks'))

    parser = argparse.ArgumentParser(description='agent')
    parser.add_argument('-H', action='store', metavar='<host>', help='couchdb hostnaname', default='localhost')
    args = vars(parser.parse_args())
    __COUCH_DB_SRV = "http://{0}:{1}".format(args['H'], '5984')

    try:
        go()
    except KeyboardInterrupt:
        pass

