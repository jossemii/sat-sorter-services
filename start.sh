python3 /satrainer/start.py &
P1=$!
python3 /satrainer/regresion.py &
P2=$!
wait $P1 $P2