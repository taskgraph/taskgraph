TaskNet

TaskNet is a framework for writing fault tolerent distributed applications. It assumes that application consists of a network of tasks, which are inter-connected based on certain topology. To make writing the fault tolerent application easy, framework defines a set of events that commonly arise such as parent fail/restart, children fail/restart. Framework monitors the task/node's health, and take care of restarting the failed tasks, and also pass on these event to task implementation so that it can do application dependent recovery.

An TaskNet application usually has three layers. And application implementation need to take care of two of them. 

1. TaskNet configuration in driver (main function), this is layer where application wire up the network of the task, and also specify what task need to run as each node. It then call Start on Framework so that every node will get into event loop.

2. TaskNet framework layer. This layer handles fault tolerency within the framework. It uses etcd and/or kubernetes for this purpose.

3. Task/Topology implementation. This is again in the hands of the application developer. They need implement Task interface and also topology, and figure out how they should react to parent/child dia/restart event to carry out the correct application logic.

      
An example of driver can be:

Framework mgr = framework.Framework()  
mgr.AddTask(&ParameterServerTask())  
mgr.AddTask(&DataShardTask())  
mgr.SetTaskConfig(&SimpleTreeConfig())  
mgr.SetTopology(NewTreeTopology(2, 127))  
mgr.Start()  

Note, for now, the completion of TaskNet is not defined explicitly. Instead, each application will have their way of exit based on application dependent logic. As an example, the above application can stop if task 0 stops. We have the hooks in Framework so any node can potentially exit the entire TaskNet. We also have hook in Task so that task implementation get a change to save the work. 