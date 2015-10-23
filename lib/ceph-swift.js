/*!
 * swift-ceph - CEPH Object Storage(Swift) REST client API for Node.JS
 *
 * @author agustik(https://edge.is)
 * @license MIT Style
 * Original from author firejune(to@firejune.com)
 */


/**
 * dependencies.
 */

var crypto = require('crypto')
  , request = require('request')
  , Syncrequest = require('sync-request')
  , fs = require('fs')
  , events = require('events');

var ev = new events.EventEmitter();

/**
 * Api object, holding authentication request and requests
 * @type {Object}
 */
var api = {
  /**
   * Auhenticates with Swift server, Syncrequest
   * @param  {object} options connection options
   * @return {object}         object with token, account and url
   */
   auth : function (options){
     if (this.strictSSL === false){
       process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
     }


     var params = {};
     var resp = Syncrequest('GET', options.url + '/auth/1.0', {
       headers : {
         'User-Agent' : 'Ceph compatable Swift client',
         'X-Auth-User': options.user,
         'X-Auth-Key' : options.pass
       }
     });
     if(resp.statusCode < 300 && resp.statusCode >= 200){
       params['storage'] = resp.headers['x-storage-url'];
       params['token'] = resp.headers['x-auth-token'];
       return params;
     }else{
       console.error('ERROR: Could not authenticate');
       return false;
     }
   },
   request : function (options, callback, data){
     callback = callback || function (){};
     var url = this.storage + options.path;
     var params = {
       method : options.method || 'GET',
       url : url,
       strictSSL : this.strictSSL,
       headers : {
         'X-Auth-Token': this.token
       , 'X-Storage-Token': this.token
       }
     };

     if (data){
       params.body = data;
     }
     if ('headers' in options){
       extend(params.headers, options.headers);
     }
     var req = request(params, function (err, resp){
       var responseObject = {};
       if (err){
         callback(err);
       }else{
         responseObject.url = params.url;
         responseObject.method = params.method;
         responseObject.headers = resp.headers;
         responseObject.status = resp.statusCode;
         if(/application\/json/.test(resp.headers['content-type'])){
           responseObject.data = ParseIfJSON(resp.body);
         }
        callback(null, responseObject, resp);
       }
     });

     if (options.localFile && options.localFile !== ''){

       req.pipe(fs.createWriteStream(options.localFile))

        req.on('end', function () {
          // Send event, so we can fetch it
          ev.emit('sw_rw_end', options.localFile);
        });
     }
   }
};

function ParseIfJSON(json){
  try {
    return JSON.parse(json);
  }catch(e){
    return json;
  }
};

/**
 * Swif init client
 * @param {object} options connections options
 */
function Swift(options) {


  if(!options){
    return false;
  }
  if ( ! options.user || !options.pass || !options.url){
    return false;
  }

  this.checksum = options.checksum || false;
  this.strictSSL= options.strictSSL || true;

  this.token = '';
  this.storage = '';
  this.account = '';

  var Authenticate = api.auth(options);
  if(Authenticate){
    this.token = Authenticate.token;
    this.storage = Authenticate.storage;
    return this;
  }

  return false;
}


function extend(destination, source) {
  for (var property in source)
    destination[property] = source[property];
  return destination;
}

/**
 * Creates md5 hash from file Buffer
 * @param {Buffer} buffer   File Buffer
 * @return {string}         md5 hash
 */
function HashIt(buffer){
  return crypto
    .createHash('md5')
    .update(buffer)
    .digest('hex');
}

/**
 * List containers available
 * @param  {Function} callback Callback
 */
Swift.prototype.listContainers = function(callback) {
  api.request.call(this, {
      path: this.account + '?format=json'
  }, callback);
};

/**
 * Get account Metadata
 * @param  {Function} callback callback
 */
Swift.prototype.retrieveAccountMetadata = function(callback) {
  api.request.call(this, {
      path: this.account
    , method: 'HEAD'
  }, callback);
};


/**
 * Uploads objects to Swift
 * @param  {String}   container Container or bucket
 * @param  {stringBuffer}   local     String of buffer to Uploads
 * @param  {string}   remote    Name of remote variable
 * @param  {Function} callback  Callback when done
 */
Swift.prototype.uploadObject = function (container, local, remote, callback, meta){
  var md5="", data, value, headerKey, headers = {};
  for( var key in meta){
    value = meta[key];
    key = key.toLowerCase();
    headerKey = 'x-object-meta-' + key;
    headers[headerKey] = value;
  }

  // If local is Buffer then set data as buffer
  if (local instanceof Buffer){
    data = local;
  }else{
    // if not, then it
    data = fs.readFileSync(local);
  }
  if(this.checksum === true){
    md5 = HashIt(data);
    headers['ETag'] = md5;
  }

  api.request.call(this, {
      path: this.account + '/' + container + '/' + remote
    , method: 'PUT'
    , headers : headers
  }, callback, data);
};

/**
 * List objects in container
 * @param  {string}   container container Name
 * @param  {Function} callback  callback
 */
Swift.prototype.listObjects = function(object, callback) {
  api.request.call(this, {
      path: this.account + '/' + object + '?format=json'
  }, callback);
};

/**
 * Creates container
 * @param  {string}   container container Name
 * @param  {Function} callback  callback
 */
Swift.prototype.createContainer = function(container, callback) {
  api.request.call(this, {
      path: this.account + '/' + container
    , method: 'PUT'
  }, callback);
};

/**
 * Sets container ACL
 * @param  {string}   container container Name
 * @param  {string}   acl       container ACL
 * @param  {Function} callback  callback
 */

Swift.prototype.setContainerRead = function (container, acl, callback){

  if (Array.isArray(acl)){
    acl = acl.join(', ');
  }
  var headers = {
    'X-Container-Read' : acl
  };
  console.log(headers);

  api.request.call(this, {
      path: this.account + '/' + container
    , method: 'POST'
    , headers : headers
  }, callback);
};

/**
 * Deletes container
 * @param  {string}   container container Name
 * @param  {Function} callback  callback
 */
Swift.prototype.deleteContainer = function(container, callback) {
    api.request.call(this, {
          path: this.account + '/' + container
        , method: 'DELETE'
    }, callback);
};

/**
 * Fetch container metadata
 * @param  {string}   container container Name
 * @param  {Function} callback  callback
 */
Swift.prototype.retrieveContainerMetadata = function(container, callback) {
  api.request.call(this, {
      path: this.account + '/' + container
    , method: 'HEAD'
  }, callback);
};

 /**
  * Fetches objet and saves to disk
  * @param  {string}   container container Name
  * @param  {string}   object    object Name
  * @param  {string}   localFile [description]
  * @param  {Function} callback  callback
 */
Swift.prototype.retrieveObject = function(container, object, localFile, callback, headers) {
  callback = callback || function (){};
  headers = headers || {};
  var hash;
  ev.once('sw_rw_end', function (filename) {
    var file = fs.readFileSync(filename);
    hash = HashIt(file);
  });

  api.request.call(this, {
      localFile : localFile
    , headers : headers
    , path: this.account + '/' + container + '/' + object
  }, function (err, resp, optional){

    if(err){
      callback(err);
    }else{
      resp.match = false;
      if(hash, resp.headers.etag){
        resp.match = true;
      }
      callback(null, resp, optional);
    }
  });
};

/**
 * Deletes object
 * @param  {string}   container container Name
 * @param  {string}   object    object Name
 * @param  {Function} callback  callback
 */
Swift.prototype.deleteObject = function(container, object, callback) {
  api.request.call(this, {
      path: this.account + '/' + container + '/' + object
    , method: 'DELETE'
  }, callback);
};

/**
 * Fetches metadata for object
 * @param  {string}   container container Name
 * @param  {string}   object    object Name
 * @param  {Function} callback  callback
 */
Swift.prototype.retrieveObjectMetadata = function(container, object, callback) {
  api.request.call(this, {
      path: this.account + '/' + container + '/' + object
    , method: 'HEAD'
  }, callback);
};

/**
 * Updates metadata for object
 * @param  {string}   container container Name
 * @param  {string}   object    object Name
 * @param  {object}   meta      object container metadata
 * @param  {Function} callback  callback
 */
Swift.prototype.updateObjectMetadata = function(container, object, meta, callback) {
  var value, headerKey, headers = {};
  for( var key in meta){
    value = meta[key];
    key = key.toLowerCase();
    headerKey = 'x-object-meta-' + key;
    headers[headerKey] = value;
  }

  api.request.call(this, {
      path: this.account + '/' + container + '/' + object
    , method: 'POST'
    , headers : headers
  }, callback);
};

/**
 * Updates metadata for container
 * @param  {string}   container container Name
 * @param  {string}   object    object Name
 * @param  {object}   meta      object container metadata
 * @param  {Function} callback  callback
 */
Swift.prototype.updateContainerMetadata = function(container, meta, callback) {
  var value, headerKey, headers = {};
  for( var key in meta){
    value = meta[key];
    key = key.toLowerCase();
    headerKey = 'x-container-meta-' + key;
    headers[headerKey] = value;
  }
  console.log(headers);

  api.request.call(this, {
      path: this.account + '/' + container
    , method: 'POST'
    , headers : headers
  }, callback);
};

/**
 * Copy object from one container to atnother
 * @param  {string}   sourceContainer      Name of container || bucket
 * @param  {string}   sourceObject         Name of object
 * @param  {string}   destinationcontainer Name of container || bucket
 * @param  {string}   destinationobject    Name of object
 * @param  {Function} callback             callback
 */
Swift.prototype.copyObject = function(sourceContainer, sourceObject,
  destinationcontainer, destinationobject, callback) {
  api.request.call(this, {
      path: this.account + '/' + destinationcontainer + '/' + destinationobject
    , method: 'PUT'
    , headers: {
      'X-Copy-From': sourceContainer + '/' + sourceObject
    }
  }, callback);
};

/**
 * Moves object from one container to another
 * @param  {string}   sourceContainer      Name of container || bucket
 * @param  {string}   sourceObject         Name of object
 * @param  {string}   destinationcontainer Name of container || bucket
 * @param  {string}   destinationobject    Name of object
 * @param  {Function} callback             callback
 */
Swift.prototype.moveObject = function(sourceContainer, sourceObject,
  destinationcontainer, destinationobject, callback) {
    callback = callback || function (){};
  var self = this;
  // first copyObject, then delete it.
  self.copyObject(sourceContainer, sourceObject, destinationcontainer, destinationobject, function(err, result) {
    if(err){
      callback(err);
    }else{
      self.deleteObject(sourceContainer, sourceObject, callback);
    }
  });
};

module.exports = Swift;
