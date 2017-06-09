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
  , events = require('events')
  , _log = require('./log.js')
  , CacheFile = ''
  , CacheTTL = 0;

var ev = new events.EventEmitter();




/**
 * Error codes for each level, Account -> Container -> Object
 * @param {string}  level  At what level is the call? account, container or object
 * @param {int}     code   Error code in 400-599
 * @param {string} action  Method + URI
 */
var ErrorsMessages = function (level, code, action){
  code = code.toString();
  var message = null;
  var messages = {
    account : {
      '401' : 'Unauthorized'
    },
    container : {

      '404' : 'Container not found',
      '409' : 'Conflict! Container not empty'
    },
    object : {
      '400' : 'Bad request',
      '401' : 'Unauthorized',
      '404' : 'Not found',
      '408' : 'Timeout',
      '411' : 'Content length required, missing Transfer-Encoding or Content-Length request header.',
      '422' : 'Sent ETag does not match remote ETag.'
    }
  };
  if (level in messages){
    if (code in messages[level]){
      message = messages[level][code];
    }
  }
  if (message){
    return {statusCode : code, message : message, level : level, action : action};
  }

  return null;


};


/**
 * Api object, holding authentication api and requests api
 * @type {Object}
 */
var api = {
  /**
   * Auhenticates with Swift server, Syncrequest
   * @param  {object} options connection options
   * @return {object}         object with token, account and url
   */
   auth : function (options){
     // Hack to disable SSL for sync-request
     if (this.strictSSL === false){
       process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
     }
     var params = {};
     var resp = Syncrequest('GET', options.url + options.authenticationpath, {
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
       _log.error('Authentication')('ERROR: Could not authenticate');
       return false;
     }
   },
   request : function (options, callback, data){
     callback = callback || function (){};
     var self = this;

     var path = URLencodePath(options.path);

     var query = buildQuery(options.query);

     var url = self.storage + path + query
     var params = {
       method : options.method || 'GET',
       url : url,
       strictSSL : self.strictSSL,
       headers : {
          'User-Agent' : 'Ceph compatable Swift client'
        , 'X-Auth-Token': self.token
        , 'X-Storage-Token': self.token
        , 'Accept' : 'application/json'
       }
     };

     if (data){
       params.body = data;
     }
     if ('headers' in options){
       extend(params.headers, options.headers);
     }
     var req = request(params, function (){});

     if (options.localFile && options.localFile !== ''){
       req.pipe(fs.createWriteStream(options.localFile))
     }
     req.on('complete', function (response, body) {

        var responseObject = {};

        var SwiftErrorCode = ErrorsMessages(options.level, response.statusCode, params.method + ' : ' + params.url);
        if(SwiftErrorCode){
          callback(SwiftErrorCode);
        }else{
          responseObject.url = params.url;
          //responseObject.method = params.method;
          responseObject.headers = response.headers;
          responseObject.status = response.statusCode;
          hash = responseObject.headers.etag;
          //console.log(response)
          if(/application\/json/.test(response.headers['content-type'])){
            responseObject.data = ParseIfJSON(response.body);
          }
          if(options.localFile && options.localFile !== '' && response.headers['content-type'] === 'binary/octet-stream' && self.checksum){
            var file = fs.readFile(options.localFile, function (err, buffer){
            var hash = HashIt(buffer);
            if (hash == response.headers.etag){
              responseObject.localHash = hash;
              callback(null, responseObject)
            }else{
              callback({
                message : 'Local and remote md5 hash does not match',
                remote : response.headers.etag,
                local : hash
              });
            }

          });
          }else{
            callback(null, responseObject);
          }
        }
     });
     req.on('error', function (error){
      _log.error('API request')(error);
      callback(error);
     });

   }
};

function buildQuery (obj) {
  var query = '';
  if (!obj){
    return '';
  }
  var arr = [];
  for ( var key in obj){
    var value = obj[key];
    arr.push(key + "=" + value);
  }
  query = arr.join('&');
  return '?' + query;
};

function URLencodePath(path){
 // urlencode path
 var parts;
 if (path.indexOf('?') > -1){
   var uriargs = path.split('?');
   var args = uriargs[1];
   path = uriargs[0];
 }

 if (path.indexOf('/') > -1){
   parts = path.split('/');
 }else{
   return path;
 }

 var encoded = parts.map(function (part){
   return enc(part);
 });

 var path = encoded.join('/');

 return path;
}

function enc(uri){
  return encodeURIComponent(uri);
}

function ParseIfJSON(json){
  try {
    return JSON.parse(json);
  }catch(e){
    return json;
  }
};

var cache = {
  read: function (key){
    var self = this;
    var CacheContent = self._readCacheFile();
    if (CacheContent){
      if (key in CacheContent){
        var now = new Date().getTime();
        if(now < CacheContent[key]._EXPIRES){
          return CacheContent[key];
        }else{
          // Delete cache
          self._deleteKey(key);
        }
      }
    }
    return false;
  },
  write : function (key, obj){
    var self = this;
    var CacheContent = self._readCacheFile();
    var now = new Date().getTime();
    obj._EXPIRES = now + (CacheTTL * 1000);
    if (CacheContent){
      CacheContent[key] = obj;
      self._writeCacheFile(CacheContent);
      if ('__cache__' in CacheContent){
       if ((now + 100000) > CacheContent['__cache__']){
         self._clean();
       }
      }
    }
  },
  _initCache : function (){
    var now = new Date().getTime();
    var json =  JSON.stringify({ __cache__ : now }, null, 2);
    fs.writeFileSync(CacheFile, json);
    return json;
  },
  _parseCache : function (string){
    try {
      return JSON.parse(string);
    } catch (e) {
      return false;
    }
  },
  _deleteKey : function (key){
    var self = this;

    var content = self._readCacheFile();
    if (content){
      if (key in content){
        delete content[key];
        self._writeCacheFile(content);
        return true;
      }
    }
    return false;
  },
  _readCacheFile : function (){
    var self = this;
    try {
      var json = fs.readFileSync(CacheFile);
      var obj =  self._parseCache(json);
      return obj;
    } catch (e) {
      self._initCache();
      return false;
    }
  },
  _writeCacheFile : function(content){
    fs.writeFileSync(CacheFile, JSON.stringify(content, null, 2));
  },
  _clean : function (){
    var self = this;
    var CacheContent = self._readCacheFile();
    var now = new Date().getTime();
    for ( var key in CacheContent ){
      var value = CacheContent[key];
      if (typeof value === 'object'){
       if (now > value._EXPIRES){
         delete CacheContent[key];
       }
      }
    }
    CacheContent['__cache__'] = now;
    self._writeCacheFile(CacheContent);

  }
};


/**
 * Swif init client
 * @param {object} options connections options
 */
function Swift(options) {


  if(!options){
    return null;
  }
  if ( ! options.user || ! options.pass || ! options.url){
    return null;
  }

  if (!options.cache && options.cache !== false){
    options.cache = {};
  }


  if (options.cache!==false){
    options.cache.enabled = options.cache.enabled || false;
    CacheTTL = options.cache.ttl || 3600;
    CacheFile = options.cache.file || '__swift_auth_cache.json';
  }

  options.authenticationpath = options.authenticationpath || '/auth/1.0';

  this.checksum = options.checksum || false;
  this.strictSSL= options.strictSSL || true;

  this.cached = false;
  this.token = '';
  this.storage = '';
  this.account = '';
  var CacheKey = HashIt(JSON.stringify(options));


  var authCache = cache.read(CacheKey);


  if (authCache){
    var n = new Date().getTime();
    this.cached = true;
    this.token = authCache.token;
    this.storage = authCache.storage;
    return this;
  }

  var Authenticate = api.auth(options);
  if(Authenticate){
    this.token = Authenticate.token;
    this.storage = Authenticate.storage;
    if(options.cache !==false){
      cache.write(CacheKey, this);
    }

    return this;
  }

  return null;
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
Swift.prototype.listContainers = function(query, callback) {
  if(typeof query === 'function'){
    callback = query;
    query = {};
  }
  api.request.call(this, {
      path: this.account
    , query : query
    , level : 'account'
  }, callback);
};

/**
 * Get account Metadata
 * @param  {Function} callback callback
 */
Swift.prototype.retrieveAccountMetadata = function(callback) {
  api.request.call(this, {
      path: this.account
    , level : 'account'
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
Swift.prototype.uploadObject = function (container, local, remote, meta, callback){
  if (typeof meta === 'function'){
    callback = meta;
    meta = {};
  }
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
    , level : 'object'
    , method: 'PUT'
    , headers : headers
  }, callback, data);
};

/**
 * List objects in container
 * @param  {string}   container container Name
 * @param  {Function} callback  callback
 */
Swift.prototype.listObjects = function(container, query, callback) {
  if (typeof query === 'function'){
    callback = query;
    query = {};
  }
  api.request.call(this, {
      path: this.account + '/' + container
    , query : query
    , level : 'container'
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
    , level : 'container'
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

  api.request.call(this, {
      path: this.account + '/' + container
    , level : 'container'
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
        , level : 'container'
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
    , level : 'container'
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
Swift.prototype.retrieveObject = function(container, object, localFile, headers, callback) {
  if (typeof headers ==='function'){
    callback = headers;
  }
  callback = callback || function (){};
  headers = headers || {};
  api.request.call(this, {
      localFile : localFile
    , level : 'object'
    , headers : headers
    , path: this.account + '/' + container + '/' + object
  }, callback);
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
    , level : 'object'
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
    , level : 'object'
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
    , level : 'object'
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
  api.request.call(this, {
      path: this.account + '/' + container
    , level : 'container'
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
Swift.prototype.deleteContainerMetadata = function(container, meta, callback) {
  var value, headerKey, headers = {};
  for( var key in meta){
    value = meta[key];
    key = key.toLowerCase();
    headerKey = 'x-remove-container-meta-' + key;
    headers[headerKey] = 'x';
  }
  api.request.call(this, {
      path: this.account + '/' + container
    , level : 'container'
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
    , level : 'object'
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
