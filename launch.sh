#!/bin/sh
SRC_PATH=`dirname $0`/src
LUA_EXECUTABLE=luajit
PWD=`pwd`

if [ "$1" != "" ]; then
    CONTENT_PATH=`realpath $1`

    if [ -d "$CONTENT_PATH" ]; then
        cd $SRC_PATH
        $LUA_EXECUTABLE magi.lua $CONTENT_PATH
        EXIT_CODE=$?
        cd $PWD
        exit $EXIT_CODE
    else
        echo "ERROR: specified path($CONTENT_PATH) must be a valid directory."
        EXIT_CODE=1
    fi
fi

echo "Usage: $0 CONTENT_PATH"
exit $EXIT_CODE
