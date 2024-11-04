#!/bin/bash

set -e

PINK_PATH=$PWD

# We depend on slash
SLASH_PATH=$1
if test -z $SLASH_PATH; then
  SLASH_PATH=$PINK_PATH/third/slash
fi

if [[ ! -d $SLASH_PATH ]]; then
  echo "Slash library is not available"
  exit 1
fi
cd $SLASH_PATH/slash && make
UNAME_S=`uname -s`
if test "x$UNAME_S" = "xDarwin" ; then
  export C_INCLUDE_PATH=/opt/homebrew_include
  export CPLUS_INCLUDE_PATH=/opt/homebrew/include
fi
# Compile pink
cd $PINK_PATH
make SLASH_PATH=$SLASH_PATH
cd rondis && make SLASH_PATH=$SLASH_PATH
