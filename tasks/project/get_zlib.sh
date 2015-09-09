#!/bin/bash

WORKDIR=$1

cd $WORKDIR || exit 1
wget http://zlib.net/zlib-1.2.8.tar.gz || exit 1
tar xvzf zlib-1.2.8.tar.gz || exit 1
