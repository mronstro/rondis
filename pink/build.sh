#!/bin/bash

set -e

PINK_PATH=$PWD
SLASH_PATH=$PINK_PATH/third/slash
cd $SLASH_PATH/slash && make
UNAME_S=`uname -s`
if test "x$UNAME_S" = "xDarwin" ; then
  export C_INCLUDE_PATH=/opt/homebrew_include
  export CPLUS_INCLUDE_PATH=/opt/homebrew/include
fi
# Compile pink
cd $PINK_PATH
make SLASH_PATH=$SLASH_PATH
