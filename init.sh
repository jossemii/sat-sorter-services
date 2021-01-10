#!/usr/bin/env bash
set -e
python3 /satrainer/start.py &
python3 /satrainer/regresion.py &
wait -n
