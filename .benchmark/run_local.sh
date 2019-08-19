#!/bin/bash

cd sidekiq/

redis-server \
& rails server -b 0.0.0.0 -p 3000 -e development \
&& fg

