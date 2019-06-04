#!/bin/bash
# -*- compile-command: "./test.sh"; -*-
set -e

export EXEC_DIR=$(pwd)/out/exec
export WORK_DIR=$(pwd)/out/work

rm -Rf out
mkdir -p out/exec/{run,log,queue} out/work
coffee exec.coffee &
sleep 1s
ls out/exec/run || :

echo 'echo $JOBNO && sleep 1s && touch first' > out/exec/queue/first

while true ; do
  sleep 0.5s
  echo "--- $(ls out/exec/run/0)"
done &

# too many jobs do not crash
sleep 1s
for I in {0..4} ; do
  echo 'echo $JOBNO'" && sleep 1s && touch first$I" > out/exec/queue/first$I
done

wait
