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
    , key = [this._prefix, domain, encodeURIComponent(type)].join(':');

  this._client.get(key, function (err, uid) {
    if (err) { return cb(err); }
    if (uid) {
      return cb(null, uid);
    }
    
    return cb(new NotFoundError('No records for "' + type + '"'))
  });
}

Registry.prototype.domains = function(cb) {
  var args = '*'
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
  var args = '*'
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
