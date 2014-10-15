var EventEmitter = require('events').EventEmitter
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
}

Registry.prototype.services = 
Registry.prototype.types = function(domain, cb) {
}

module.exports = Registry;
