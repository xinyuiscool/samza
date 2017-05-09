---
layout: page
title: Fluent API Guide
---
<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

This page shows you how to run a Samza stream application with fluent API under different environments.

### Architecture Overview

Above diagram shows an overview of Apache Samza architecture. There are four layers in the architecture:

#### I. Fluent API

Samza fluent API provides a unified way to handle both streaming and batch data. It provides operators like map, filter, window and join to allow the user to describe the whole end-to-end data processing in a single program. It can consume data from various sources and publish to different sinks. The details of Samza fluent API is covered in (link).

#### II. ApplicationRunner

During run time, Samza uses ApplicationRunner to execute a stream application. The runner will generate the necessary configs such as input/output streams, create intermediate streams for partitionBy(), and run the Samza processor in two environments:

* RemoteApplicationRunner - submit the application to a remote cluster which runs it in remote JVMs. This runner is invoked via run-app.sh script. 
* LocalApplicationRunner - runs the application in the local JVM processes. This runner is directly invoked by the users in their application’s main() method.

To use RemoteApplicationRunner, config the following property with your StreamApplication class:

{% highlight jproperties %}
# The StreamApplication class to run
app.class=Your.StreamApplication.Class
{% endhighlight %}

Then you can use run-app.sh to run the application in remote cluster as described in this tutorial (link).

To use LocalApplicationRunner, you can run your StreamApplication with the runner in your program. The following shows an example of how to run it in main():

{% highlight java %}
public static void main(String[] args) throws Exception {
 CommandLine cmdLine = new CommandLine();
 Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
 // Create the new LocalApplicationRunner instance
 LocalApplicationRunner localRunner = new LocalApplicationRunner(config);
 // Run your StreamApplication in local JVM
 StreamApplication app = new YourStreamApplication();
 localRunner.run(app);
 
 // Wait for the application to finish
 localRunner.waitForFinish();
 System.out.println("Application completed with status " + localRunner.status(app));
}
{% endhighlight %}

#### III. Deployment

Samza supports two types of deployment models: remote deployment and local deployment. In remote deployment, a cluster will run Samza application in distributed containers and manage their life cycle. Right now Samza only support Yarn cluster deployment.

In local deployment, the users can use Samza as a library and run stream processing in their program with any kind of the clusters, like Amazaon EC2 or Mesos. For this deployment, Samza can be configured to use two kinds of coordinator among the cluster:

* Zookeeper - Samza uses Zookeeper to manage group membership and partition assignment. This allows the users to scale in run time by adding more JVMs.
* Standalone - Samza can run the application in a single JVM locally without coordination, or multiple JVMs using the user-configured task-to-container and partition-to-task groupers. This supports static user-defined partition assignment.   

To use Zookeeper-based coordination, the following configs are required:

{% highlight jproperties %}
job.coordinator.factory=org.apache.samza.zk.ZkJobCoordinatorFactory
job.coordinator.zk.connect=yourzkconnection
{% endhighlight %}

To use standalone coordination, the following configs are needed:

{% highlight jproperties %}
job.coordinator.factory=org.apache.samza.standalone.StandaloneJobCoordinatorFactory
{% endhighlight %}

For more details of local deployment using Zookeeper, please take a look at this tutorial (link).

#### IV. Processor

The finest execution unit of a Samza application is the StreamProcessor, which runs stream processing in a single thread. It reads the configs generated from the ApplicationRunner, and consumes the input stream partitions assigned by the JobCoordinator. It can access local state data in either RocksDb or memory, and access remote data efficiently using multithreading. 