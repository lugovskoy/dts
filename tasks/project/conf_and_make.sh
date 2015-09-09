#!/bin/bash

WORKDIR=$1
OUTDIR=$2
LOG=$3
TCC=$4

cd $WORKDIR

CC=$TCC ./configure --prefix $OUTDIR/zlib 1>>$LOG 2>&1 || exit 1
make -j8 1>>$LOG 2>&1 || exit 1
make install 1>>$LOG 2>&1 || exit 1
