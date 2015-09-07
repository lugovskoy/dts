#!/bin/bash

WORKDIR=$1
OUTDIR=$2
LOG=$3

cd $WORKDIR

./configure --prefix $OUTDIR --enable-languages=c 1>>$LOG 2>&1 || exit 1
make -j8 1>>$LOG 2>&1 || exit 1
make install 1>>$LOG 2>&1 || exit 1
