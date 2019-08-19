cd vapor/

swift run Run backgrounder &
PID=$!

while true
do
  sleep 1
  echo $(ps -p "$PID" -o %cpu,%mem)
done
