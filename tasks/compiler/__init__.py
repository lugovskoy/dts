#! /usr/bin/env python
# coding=utf-8

import os.path
import subprocess


class SubClass():
    def __init__(self, output_dir, log):
        self.__output_dir = output_dir
        self.__log = log
        self.__wd = os.path.dirname(os.path.realpath(__file__))
        self.__init_done = False


    def is_initialized(self):
        return self.__init_done


    def initialize(self):
        script = os.path.join(self.__wd, 'get_gcc.sh')
        retcode = subprocess.call([script, self.__wd])
        self.__init_done = retcode == 0


    def is_enabled(self):
        return True


    def __result(self, output_dir, retcode):
        return {
          'gcc': os.path.join(output_dir, 'bin'),
          'passed': retcode == 0
        }


    def run(self, q, args):
        script = os.path.join(self.__wd, 'conf_and_make.sh')
        retcode = subprocess.call([script, self.__wd, self.__output_dir, self.__log])
        q.put({'retcode': retcode, 'result': self.__result(self.__output_dir, retcode)})


