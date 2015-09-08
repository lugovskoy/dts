#!/bin/bash

WORKDIR=$1

cd $WORKDIR || exit 1
wget http://gcc.cybermirror.org/releases/gcc-5.2.0/gcc-5.2.0.tar.bz2 || exit 1
tar xvjf gcc-5.2.0.tar.bz2 || exit 1
cd gcc-5.2.0 || exit 1
./contrib/download_prerequisites || exit 1
