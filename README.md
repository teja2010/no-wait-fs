# no-wait-fs
Distributed FS with wait-free ops

The application that we want to build is a HDFS like distributed file system that lets users create, modify and run operations with the following guarantees: 

1. While a client is running read operations, the file's contents will *appear* not to modify. 
2. Multiple readers can read the file, but they need not be reading the same version of the file. 
3. Atmost one writer can modify the file at any instant. 
4. Clients reading the file are not blocked by client modifying the file. 
5. Client writing the file is not blocked by clients reading the file. 


## installing zookeeper
1. sudo apt install default-jdk

2. wget http://apache.mirrors.hoobly.com/zookeeper/zookeeper-3.6.1/apache-zookeeper-3.6.1-bin.tar.gz

3. setup config:

```
        # The number of milliseconds of each tick
        tickTime=2000
        # The number of ticks that the initial 
        # synchronization phase can take
        initLimit=10
        # The number of ticks that can pass between 
        # sending a request and getting an acknowledgement
        syncLimit=5

        # the directory where the snapshot is stored.
        # do not use /tmp for storage, /tmp here is just 
        # example sakes.
        dataDir=/home/ubuntu/zookeeper_dataDir1

        # the port at which the clients will connect
        clientPort=2181
        initLimit=5
        syncLimit=2

        # set the right server ids <--------- change these values
        server.1=127.0.0.1:2888:3888
        server.2=127.0.0.1:2898:3898
        server.3=127.0.0.1:2878:3878
```

4. ./bin/zkServer.sh --config conf/zoo start
