#!/bin/bash

WORKDIR=$1

cd $WORKDIR || exit 1
wget http://download.savannah.gnu.org/releases/tinycc/tcc-0.9.26.tar.bz2 || exit 1
tar xvjf tcc-0.9.26.tar.bz2 || exit 1
