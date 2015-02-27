TaskGraph
=========

[![Build Status](https://travis-ci.org/taskgraph/taskgraph.svg)](https://travis-ci.org/taskgraph/taskgraph)

TaskGraph is a framework for writing fault tolerent distributed ML applications. It assumes that application consists of a network of tasks, which are inter-connected based on certain topology (hence graph). TaskGraph assume for each task (logical unit of work), there are one primary node, and zero or more backup nodes. TaskGraph then help with two types of node failure: failure happens to nodes from different task, failure happens to the nodes from the same task. Framework monitors the task/node's health, take care of restarting the failed tasks, and continue the job in a fault tolerant manner.

TaskGraph supports a channel-processor model for communication. Usually a task would want some data from other tasks, and provide computed results that other tasks want. This is done by composing a processor which has in and out channels. Once a processor gets all the data from in channels, it will do user-implemented computation. The computation result will be sent (by user) to out channels which are connected to other processors. There is a special kind of processor which doesn't have any out channels, called sync point. Once a sync point has finished its work, an iteration is finished.

Many ML jobs are iterative. In TaskGraph, it's an epoch for one iteration. TaskGraph will call user-implemented function at the beginning of each epoch to compose an inter-connected channels and processors graph. Once an iteration is done, TaskGraph will start a new iteration until user shutdown the job.

An TaskGraph application usually has three layers:

1. In driver (main function), applicaiton need to configure the TaskBuilder, which specifies what task needs to be run and what it does. Finally, user will start the job.

2. TaskGraph framework handles fault tolerance within the framework. It uses etcd and/or kubernetes for this purpose.

3. Application need to implement Task interface, to specify what they do for work for each epoch.

For an example of driver and task implementation, check example/ directory.