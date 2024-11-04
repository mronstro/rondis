#!/bin/bash

set -e

PINK_PATH=$PWD
SLASH_PATH=$PINK_PATH/third/slash
cd $SLASH_PATH/slash && make

# Compile pink
cd $PINK_PATH
SLASH_PATH=$SLASH_PATH make
