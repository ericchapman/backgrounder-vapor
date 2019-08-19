cd sidekiq/
bundle exec sidekiq -e development -C config/sidekiq.yml &
PID=$!

while true
do
  sleep 1
  echo $(ps -p "$PID" -o %cpu,%mem)
done
