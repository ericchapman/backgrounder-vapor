# Backgrounder

**Backgrounder** is an extremely fast and light weight job manager for Vapor.
It is built using Redis and inspired by Ruby's [Sidekiq](https://github.com/mperham/sidekiq).
It is also fully compatible and can interoperate with Sidekiq.

## Overview

**Backgrounder** utilizes Vapor's EventLoop architecture to create a scalable 
infrastructure that supports multiple processes, each with multiple workers,
where each worker is capable of executing multiple jobs.

 - processes - Refers to an executing Vapor instance
 - workers - Refers to threads inside a process
 - jobs - Refers to jobs executing inside a worker

The jobs inside a worker share the same EventLoop so they should be utilizing
the Future/Promise architecture of Vapor and not synchronously waiting for items
to finish.  This will allow other jobs to be processed while a job is waiting for
data from say a database or Redis.

## Scalability

Each process will spawn X workers at startup.  Each worker will poll Redis
looking for new jobs.  As jobs are dispatched, the worker will execute them on
the event loop, limiting it to Y jobs per event loop.

So one worker can support X\*Y jobs executing simultaneously.  These values are
configurable using the "concurrency" and "maxJobsPerWorker" configuration
attributes respectively.

## License
MIT

## Documentation
Please see the [Wiki](https://github.com/ericchapman/backgrounder-vapor/wiki) for more details.
