var EventEmitter = require('events').EventEmitter
  , NotFoundError = require('./errors/notfounderror')
  , redis = require('redis')
  , uuid = require('node-uuid').v4
  , util = require('util')
  , debug = require('debug')('sd-redis');

function Registry(options) {
  console.log(options)
  this._host = options.host;
  this._port = options.port;
  this._db = options.db;
  this._prefix = options.prefix || 'srv';
}

util.inherits(Registry, EventEmitter);

Registry.prototype.connect = function(options, readyListener) {
  if (readyListener) { this.once('ready', readyListener); }
  
  var self = this;
  this._client = redis.createClient(this._port, this._host);
  if (this._db) {
    this._client.select(this._db);
  }
  
  this._client.once('ready', function() {
    self.emit('ready');
  });
  this._client.on('close', this.emit.bind(this, 'close'));
  this._client.on('error', this.emit.bind(this, 'error'));
}

Registry.prototype.close = function () {
}

Registry.prototype.announce = function(domain, type, data, options, cb) {
  if (typeof options == 'function') {
    cb = options;
    options = undefined;
  }
  options = options || {};
  
  if (typeof data == 'object') {
    data = JSON.stringify(data);
  }
  var uid = options.id || uuid();
  var ttl = options.ttl || 86400;
  
  
  var key = [ this._prefix, domain, encodeURIComponent(type), uid ].join(':');
  var args = [ key, data, 'EX', ttl ];
  debug('setting %s = %s, ttl %d', key, data, ttl);
  this._client.set(args, function(err) {
    if (err) { return cb(err); }
    return cb(null);
  });
  
  return uid;
}

Registry.prototype.unannounce = function(domain, type, uid, cb) {
}

Registry.prototype.resolve = function(domain, type, cb) {
  var self = this
    , kSet = []
    , joinKey = [this._prefix, domain, encodeURIComponent(type)].join(':')
    , args =  joinKey + ':*';
  this._client.keys(args, function (err, keys) {
    var kIdx = 0
    function kIter(err) {
      if (err) { return kIter(); }
      var curKey = keys[kIdx++];
      if (! curKey) {
        return cb(null, kSet);
      }

      self._client.get(curKey, function (err, value) {
        if (err) { return kIter(err); }
        var json;
        try {
          json = JSON.parse(value);
          kSet.push(json);
        } catch (_) {
          kSet.push(value);
        }
        return kIter();
      });
    }
    kIter()
  });
}

Registry.prototype.domains = function(cb) {
  var args = [this._prefix, '*'].join(':')
    , self = this
    , kSet = {};
  this._client.keys(args, function(err, keys) {
    if (err) { return cb(err); }
    keys.forEach(function (key) {
      key = key.split(':');
      if (key[0] === self._prefix) {
        if (key[1]) {
          kSet[key[1]] = true;  // This is the domain.
        }
      }
    });
    return cb(null, Object.keys(kSet));
  });
}

Registry.prototype.services = 
Registry.prototype.types = function(domain, cb) {
  var args = [this._prefix, domain, '*'].join(':')
    , self = this
    , kSet = {};
  this._client.keys(args, function(err, keys) {
    if (err) { return cb(err); }
    keys.forEach(function (key) {
      key = key.split(':');
      if (key[0] === self._prefix) {
        if (key[1] === domain) {
          if (key[2]) {
            kSet[decodeURIComponent(key[2])] = true;  // This is the type
          }
        }
      }
    });
    return cb(null, Object.keys(kSet));
  });
}

module.exports = Registry;
