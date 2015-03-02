TaskGraph
=========

[![Build Status](https://travis-ci.org/taskgraph/taskgraph.svg)](https://travis-ci.org/taskgraph/taskgraph)

TaskGraph is a framework for writing fault tolerent distributed ML applications. It assumes that application consists of a network of tasks, which are inter-connected based on certain topology (hence graph). TaskGraph assume for each task (logical unit of work), there are one primary node, and zero or more backup nodes. TaskGraph then help with two types of node failure: failure happens to nodes from different task, failure happens to the nodes from the same task. Framework monitors the task/node's health, take care of restarting the failed tasks, and continue the job in a fault tolerant manner.

TaskGraph supports a channel-joint model. Usually a task would want some data from other tasks, compute based on the data, and provide the result to other tasks. This is done by composing some joints doing the work and the channels they get and send data. Once a joint gets all the data it needs, it calls user-implemented compute function. There is a special kind of joint which doesn't have any out channels, called sync point. Once a sync point has finished its work, an iteration is finished. Every joint cares about its local work. But in overall thinking, there is a network of joints/tasks getting data computed and flown across one by one.

Many ML jobs are iterative. In TaskGraph, it's an epoch for one iteration. TaskGraph will call user-implemented function at the beginning of each epoch to compose a specification mentioned above. Fault tolerance, consistency are handled by framework. Once an iteration is done, TaskGraph will start a new iteration unless user shutdown the job.

An TaskGraph application usually has three layers:

1. In driver (main function), applicaiton need to configure the TaskBuilder, which specifies what task needs to be run and what it does. Finally, user will start the job.

2. TaskGraph framework handles fault tolerance within the framework.

3. Application need to implement Task interface, to specify what they do for work for each epoch.

For an example of driver and task implementation, check example/ directory.