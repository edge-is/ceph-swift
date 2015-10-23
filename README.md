# Swift

OpenStack Object Storage(Swift) REST client API for Node.JS

Works with CEPH!

### Installing

### Code
```javascript
    var Swift = require('swift');

    var client = new Swift({
        user: 'username'
      , pass: 'access_key'
      , url: 'http://swift.server.com'
      , checksum : true
      , strictSSL : true
    });

    // Authentication
    client.listContainers(callback);
    client.retrieveAccountMetadata(callback);

    // Storage Services
    client.listObjects("containerName", callback);
    client.createContainer("containerName", callback);
    client.deleteContainer("containerName", callback);
    client.retrieveContainerMetadata("containerName", callback);
    client.setContainerRead("containerName", "ACL", callback);

    // Object Services
    client.retrieveObject("containerName", "objectName", "localFile", callback);
    client.createObject("containerName", "objectName", callback);
    client.uploadObject("containerName", "localFile", "RemoteFile", callback);
    client.copyObject("srcContainer", "srcObject", "dstContainer", "dstObject", callback);
    client.deleteObject("containerName", "objectName", callback);
    client.retrieveObjectMetadata("containerName", "objectName", callback);
    client.updateObjectMetadata("containerName", "objectName", { somekey : 'somevalue' }, callback);

```
### License

MIT <3
