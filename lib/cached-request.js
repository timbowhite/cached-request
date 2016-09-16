/*
CachedRequest class
*/
'use strict';

var fs = require("graceful-fs")
,   querystring =  require("querystring")
,   RequestMiddleware = require("./request-middleware")
,   util = require("util")
,   zlib = require("zlib")
,   Transform = require("stream").Transform
,   EventEmitter = require("events").EventEmitter
,   lo = require('lodash')
,   writeFileAtomic = require('write-file-atomic')
,   Q = require('q')
,   lockfile = require('proper-lockfile')
,   retry = require('retry');

util.inherits(Response, Transform);

function Response (options) {
  Transform.call(this, options);
};

Response.prototype._transform = function (chunk, enconding, callback) {
  this.push(chunk);
  callback();
};

util.inherits(CachedRequest, EventEmitter);

function CachedRequest (request) {
  EventEmitter.call(this);

  var self = this;

  this.request = request;
  this.cacheDirectory = "/tmp/";
  this.lockOpt = {
    stale: undefined,
    update: undefined,
    retry: { 
      retries: 5,
      factor: 3,
      minTimeout: 50,
      maxTimeout: 10 * 1000,
      randomize: true 
    }
  };

  function _request () {
    return self.cachedRequest.apply(self, arguments);
  }

  _request.get = function () {
  	arguments[0].method = 'GET';

    return self.cachedRequest.apply(self, arguments);
  }

  _request.setCacheDirectory = function (cacheDirectory) {
    self.setCacheDirectory(cacheDirectory);
  }

  _request.setValue = function (key, value) {
    self[key] = value;
  }

  _request.getValue = function (key) {
    return self[key];
  }

  _request.on = function (event, handler) {
    self.on(event, function () {
      handler.apply(_request, arguments);
    });
  }

  return _request;
};

CachedRequest.prototype.setCacheDirectory = function (cacheDirectory) {
  cacheDirectory ? this.cacheDirectory = cacheDirectory : void 0;
  if (this.cacheDirectory.lastIndexOf("/") < this.cacheDirectory.length - 1) {
    this.cacheDirectory += "/";
  };
};

CachedRequest.prototype.handleError = function (error) {
  if (this.logger) {
    this.logger.error({err: error});
  } else {
    console.error(error.stack);
  };
};

CachedRequest.prototype.normalizeOptions = function (options) {
  var _options = {};

  _options.method = options.method || "GET";
  _options.url = options.url || options.uri;
  _options.headers = options.headers || {};
  _options.payload = options.body || options.form || options.formData || options.json || "";

  if (options.qs) {
    _options.url += querystring.stringify(options.qs);
  };

  return _options;
};

CachedRequest.prototype.getLockOpt = function(lockOpt){
  var lockOpt = lo.clone(lockOpt || this.lockOpt);
  lo.merge(lockOpt, {
    realpath: false,
    retries: 0
  });
  return lockOpt;
}

CachedRequest.prototype.hashKey = function (key) {
  var hash = 0, i, chr, len;
  if (key.length == 0) return hash;
  for (i = 0, len = key.length; i < len; i++) {
    chr   = key.charCodeAt(i);
    hash  = ((hash << 5) - hash) + chr;
    hash |= 0;
  };
  return hash;
};

/**
 * Returns the response file path when passed the request options. 
 * 
 * @param {object} opt 
 * @return {string}
 */
CachedRequest.prototype.getResponsePath = function(opt){
  var key = this.hashKey(JSON.stringify(this.normalizeOptions(opt)));
  return this.cacheDirectory + key;
}

/**
 * Returns the meta file path when passed the request options. 
 * 
 * @param {object} opt 
 * @return {string}
 */
CachedRequest.prototype.getMetaPath = function(opt){
  return this.getResponsePath(opt) + '.json';
}

/**
 * Reads the response file from disk. If a file releaseLock function was provided, it will be called when the response
 * file is successfully opened for reading.
 * 
 * @param opt 
 * @return {object}     promise object, resolves with true if response file was read successfully, otherwise false if
 *                      response file was stale or didnt exist or metafile didnt exist
 */
CachedRequest.prototype.readFromCache = function(opt){
  var self = this
  ,   headersReader
  ,   responseReader
  ,   abort = false;

  opt = opt || {};
  lo.defaults(opt, {
    responsePath: undefined,
    metaPath: undefined,
    requestMiddleware: undefined,
    json: undefined,
    callback: undefined,
    ttl: undefined,
    releaseLock: undefined
  });

  // check if response file exists
  return Q.nfcall(fs.stat, opt.responsePath)
  // response file doesn't exist or some other error
  .fail(function(err){
    abort = true;
    if (err.code !== 'ENOENT') return Q.reject(err);
    return Q.resolve(false);
  })
  .then(function(stats){
    if (abort) return stats;

    var def = Q.defer();

    // check if it's stale
    if (stats.mtime.getTime() + opt.ttl < Date.now()) return false;

    // consider this a successful cachehit and release file lock so other commands/processes can access it
    if (typeof(opt.releaseLock) === 'function'){
      opt.releaseLock();
    }

    //Open headers file
    headersReader = fs.createReadStream(opt.metaPath);

    headersReader.on('error', function (err) {
      if (err.code === 'ENOENT'){
        err = new Error('cached-request: metafile is inaccessible, but response was not');
        err.file = opt.metaPath;
      }
      def.reject(err); 
    });

    headersReader.on("open", function (fd) {
      return Q().then(function(){
        // Open the response file
        responseReader = fs.createReadStream(opt.responsePath);

        // If it doesn't exist, response that it needs to be fetched 
        responseReader.on("error", function (err) {
          if (err.code === 'ENOENT'){
            err = new Error('cached-request: response file became inaccessible before it could be read');
            err.file = opt.responsePath;
          }
          def.reject(err); 
        });

        responseReader.on("open", function (error) {
          //Create a fake response object
          var response = new Response();
          response.statusCode = 200;
          response.headers = "";

          //Read the haders from the file and set them to the response
          headersReader.on("data", function (data) {
            response.headers += data.toString();
          });
          headersReader.on("end", function () {
            response.headers = JSON.parse(response.headers);
            //Notify the response comes from the cache.
            response.headers["x-from-cache"] = 1;
            //Emit the "response" event to the client sending the fake response
            opt.requestMiddleware.emit("response", response);

            var gzipResponse = response.headers._gzipResponse;

            var stream;
            if (response.headers['content-encoding'] === 'gzip' || ! gzipResponse) {
              stream = responseReader;
            } else {
              // Gunzip the response file
              stream = zlib.createGunzip();
              responseReader.on('error', function (error) {
                stream.end();
              });
              stream.on('error', function (error) {
                responseReader.close();
              });
              responseReader.pipe(stream);
            }

            //Read the response file
            var responseBody;
            stream.on("data", function (data) {
              //Write to the response
              response.write(data);
              //If a callback was provided, then buffer the response to send it later
              if (opt.callback) {
                responseBody = responseBody ? Buffer.concat([responseBody, data]) : data;
              }
              //Push data to the client's request
              opt.requestMiddleware.push(data);
            });

            stream.on("end", function () {
              //End response
              response.end();
              //If a callback was provided
              if (opt.callback) {
                //Set the response.body
                response.body = responseBody;
                //Parse the response body (it needed)
                if (opt.json) {
                  try {
                    responseBody = JSON.parse(responseBody.toString());
                  } catch (e) {
                    return callback(e);
                  };
                };
                //callback with the response and body
                opt.callback(null, response, responseBody);
              };
              opt.requestMiddleware.push(null);
              def.resolve(true);
            });
          });
        });
      })
      // reject on the parent promise chain
      .fail(def.reject)
      .fin(function(){
        if (headersReader) headersReader.close;
        if (responseReader) responseReader.close;
      });
    });
    return def.promise;
  }); 
}

/**
 *  
 * @param {object} opt 
 * @return {object}       promise object 
 */
CachedRequest.prototype.makeRequest = function(opt){
  var self = this;

  opt = opt || {};
  lo.defaults(opt, {
    reqargs: undefined,
    responsePath: undefined,
    metaPath: undefined,
    requestMiddleware: undefined,
    callback: undefined
  });

  var cacheEvents = new EventEmitter()
  ,   releaseLock
  ,   reqopt = opt.reqargs[0];

  cacheEvents.ended = false;

  cacheEvents.on('end', function(){ 
    cacheEvents.ended = true; 
  });

  // call the user callback once the metadata and response files have been written
  if (opt.callback){
    opt.reqargs[opt.reqargs.length - 1] = function callbackWrap(){
      var cbargs = arguments;
      if (cacheEvents.ended) return opt.callback.apply(null, cbargs); 
      cacheEvents.once('end', function(){ 
        return opt.callback.apply(null, cbargs);
      });
    };
  }

  return Q().then(function(){
    var def = Q.defer()
    ,   request = self.request.apply(null, opt.reqargs);

    opt.requestMiddleware.use(request, cacheEvents);

    request.on('error', def.reject); 

    request.on("response", function (response) {
      var contentEncoding
      ,   gzipper
      ,   meta
      ,   responseWriter;

      //Only cache successful responses
      if (! response.statusCode || response.statusCode < 200 || response.statusCode >= 300){
        request.abort();
        return def.resolve();
      }

      response.on('error', function (error) {
        self.handleError(error);
      });

      // save metadata: response headers and gzipped flag
      meta = lo.clone(response.headers);

      meta._gzipResponse = reqopt.gzipResponse;

      responseWriter = fs.createWriteStream(opt.responsePath);

      responseWriter.on('error', function (error) {
        self.handleError(error);
      });

      responseWriter.on('finish', function(){
    
        // write metadata file
        writeFileAtomic(opt.metaPath, JSON.stringify(meta), function (error) {
          if (error) self.handleError(error);
          def.resolve(); 
        });
      });

      //
      contentEncoding = response.headers['content-encoding'] || '';
      contentEncoding = contentEncoding.trim().toLowerCase();

      if (contentEncoding === 'gzip' || ! reqopt.gzipResponse) {
        response.on('error', function (error) {
          responseWriter.end();
        });
        response.pipe(responseWriter);
      } else {
        gzipper = zlib.createGzip();
        response.on('error', function (error) {
          gzipper.end();
        });
        gzipper.on('error', function (error) {
          self.handleError(error);
          responseWriter.end();
        });
        responseWriter.on('error', function (error) {
          response.unpipe(gzipper);
          gzipper.end();
        });
        response.pipe(gzipper).pipe(responseWriter);
      }
    });
    self.emit("request", opt.reqargs[0]);

    return def.promise;
  })
  // always release lockfile and signal caching is finished
  .fin(function(){
      if (typeof(opt.releaseLock) === 'function'){    
        opt.releaseLock();
      }
      cacheEvents.emit('end');
  });
}

CachedRequest.prototype.cachedRequest = function () {
  var self = this
  ,   requestMiddleware = new RequestMiddleware()
  ,   args = arguments
  ,   options = args[0]
  ,   callback
  ,   metaPath
  ,   responsePath
  ,   cachehit = false
  ,   releaseLock;

  lo.defaults(options, {
    ttl: 0, 
    gzipResponse: true,
    lockOpt: undefined  // see this.lockOpt and getLockOpt, some options need to be enforced
  });

  var lockOpt = self.getLockOpt(options.lockOpt);

  if (typeof options != "object") {
    throw new Error("An options object must provided. e.g: request(options)")
  }

  if (typeof args[args.length - 1] == "function") {
    callback = args[args.length - 1];
  };

  responsePath = this.getResponsePath(options); 
  metaPath = this.getMetaPath(options); 

  // obtain lock on response file path (atomic for multi-process) 
  Q.nfcall(lockfile.lock, responsePath, lockOpt)
  // lock file exists means response is currently being written
  .fail(function(err){
    // some other error, abort everything 
    if (err.code !== 'ELOCKED') return Q.reject(err);

    // don't wait for response to be written
    if (! lockOpt.retry) return;

    // wait for lockfile to be released
    var op = retry.operation(lockOpt.retry)
    ,   def = Q.defer();

    op.attempt(function(attempt){
      lockfile.check(responsePath, function(err, locked){
        // retry on error or still locked
        if (op.retry(err || locked)){
          return;
        }

        // no retries left, error checking for lockfile
        if (err) return def.reject(err);

        // file done being written 
        def.resolve();
      });
    });
    return def.promise;
  })
  // no lock, lock obtained, or done waiting for lock to unlock
  .then(function(r){
    releaseLock = r; 

    // try to read from cache
    return self.readFromCache({
      ttl: options.ttl, 
      responsePath: responsePath,
      metaPath: metaPath,
      requestMiddleware: requestMiddleware,
      json: options.json,
      callback: callback, 
      releaseLock: releaseLock
    });
  })
  .then(function(c){
    cachehit = c;

    // cache file exists, nothing else to do
    if (cachehit) return;
    
    return self.makeRequest({
      reqargs: args,
      callback: callback,
      releaseLock: releaseLock,
      responsePath: responsePath,
      metaPath: metaPath,
      requestMiddleware: requestMiddleware,
    });
  })
  .fail(function(err){
    self.handleError(err);
  })
  .fin(function(){
    // release lock in event of error (expecting makeRequest or readFromCache to call this already) 
    if (typeof(releaseLock) === 'function') releaseLock();
  })
  .done();

  return requestMiddleware;
};

module.exports = CachedRequest;
