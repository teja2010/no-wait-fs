# Design



### Backend RPC Interface
|Operation     | Description |
|:----------------|:-------------------------------|
| `WriteShard()`  | Write the shard to backend     |
| `Read_op()`     | Run operation on shard and return the result |


### RCU Recipe Interface
| Method        | Description |
|:----------------|:-------------------------------|
| `Create_RCU_resource()` | Returns a handle for a resource which is protected by RCU. |
| `zk_rcu.read_lock()`    | Hold a read lock on the resource |
| `zk_rcu.read_unlock()`  | Release lock. |
| `zk_rcu.dereference()`  | Read from the resouce. Must be within a read lock.
| `zk_rcu.assign()`       | set a value to the resource |

A seperate process cleans up old RCU versions of data.

```
   Create_RCU_resource(filepath) {
      if zk.Exists(filepath) {
          //check if structure is setup. children >= 2
          return zk_rcu
      }
      else {
          zk.Create(filepath)
          zk.Create(filepath+"/writer_lock")
          writer_lock()
          zk.Create(filepath+"/version", sequential, empty metadata) // creates version 0
          writer_unlock()
          return zk_rcu{filepath}
      }
   }
   
   zk_rcu.assign(version, metadata) {
      writer_lock()
      defer writer_unlock()
      
      if latest_child(zk_rcu.filepath).version == version {
          // the last version we read must be the latest child znode's version.
          return
      }
      
      zk.Create(filepath+"/version", sequential, metadata)
   }
   
   zk_rcu.read_lock() bool {
      if latest_child(zk_rcu.filepath).version == 0 {
          return false // no resource. nothing to lock
      }
      
      zk_rcu.rlock_file = zk.Create(latest_child(zk_rcu.filepath)+"/rcu", sequential|ephimeral, empty metadata)
   }
   
   zk_rcu.read_unlock(filepath) bool {
      zk.Delete(zk_rcu.rlock_file)
   }
   
   zk_rcu.dereference() {
      return zk.Get(zk_rcu.rlock_file.remove_suffix("/rcu"))
   }
```

### FS interface:

|Operation     | Description |
|:-------------|:-------------------------------|
|`Open()`      | Get a handle on the file       |
|`Read_op()`   | Run a read operation on a file |
|`Write()`     | Write changes to the FS        |
|`Write_meta()` | Write the metadata changes     |
| `Read_lock()`/ `Read_unlock()`  |  locks for reading |


```
  Open(filepath) {
      if !zk.Exists(filepath) {
          return error
      }
      
      rcu = Create_RCU_resource(filepath)
      return &fs_handle{filepath, zk_rcu}
  }
  
  fh.Read_lock() {
      fh.zk_rcu.read_lock()
 }
  fh.Read_unlock() {
      fh.zk_rcu.read_unlock()
 }
 
 fh.Read_op(operation) {
      shards = fh.zk_rcu.dereference()
      
      ret = [];
      for s in shards {
          ret += Read_op(s, op)  //RPC call to backend
      }
      return ret    
 }
 
 fh.Write(data) metdadata {
    back = get_random_backends()
    
    meta = []
    new_shards = divide_into_shards(data)
    for s in new_shards {
        for b in back {
            meta += b.Write(s)
        }
    }
    
    return meta
 }
 
 fh.Write_meta(metadata) error {
    return zk_rcu.assign(metadata)
 }
  ```
  
  ### Znodes structure
  
  ```
  /
  |-- filename1
  |   |-- writelock/
  |   |   |-- writer1
  |   |   `-- ...
  |   |
  |   |-- version000003    (has multiple readers)
  |   |   |-- rcu0000001
  |   |   `-- ...
  |   |
  |   |-- version000005    (has no reader, will be cleaned up)
  |   |
  |   `-- version000006    (the latest version. new readers will be added here)
  |
  `-- ...
  
  ```
  
  
  
  
  
  
  
  
  
  
