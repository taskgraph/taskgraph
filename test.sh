#!/bin/bash

if [ "$1" == "l" ]; then
  cat .tmp/test-log | less -i
elif [ "$1" == "" ]; then
  .script/test 2>&1 | tee .tmp/test-log
else
  echo "unknown:" $1
fi
