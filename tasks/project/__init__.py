#! /usr/bin/env python
# coding=utf-8

import os.path
import subprocess


class SubClass():
    def __init__(self, output_dir, log):
        self.__output_dir = output_dir
        self.__log = log
        self.__wd = os.path.dirname(os.path.realpath(__file__))
        self.__init_done = os.path.join(self.__wd, 'init_done')


    def is_initialized(self):
        return os.path.isfile(self.__init_done)


    def initialize(self):
        pass


    def is_enabled(self):
        return True


    def run(self, q, args):
        pass


