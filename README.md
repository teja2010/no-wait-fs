# no-wait-fs
Distributed FS with wait-free ops

The application that we want to build is a HDFS like distributed file system that lets users create, modify and run operations with the following guarantees: 

1. While a client is running read operations, the file's contents will *appear* not to modify. 
2. Multiple readers can read the file, but they need not be reading the same version of the file. 
3. Atmost one writer can modify the file at any instant. 
4. Clients reading the file are not blocked by client modifying the file. 
5. Client writing the file is not blocked by clients reading the file. 
