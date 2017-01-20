'use strict';
var debug = require('debug')('amqp-queue:view'),
  redis = require('redis');

function View (name, options) {
  if(!(this instanceof View)) {
    return new View(options);
  }
  this.options = options;
  this.jobCollectionName = options.prefix + name + ':jobs';

  this.redis = redis.createClient(options.redis);

  this.redis.on('error', function (err) {
    debug('Error ' + err);
  });
}

View.prototype.cacheKey = function (id) {
  return this.jobCollectionName + ':' + id;
};

View.prototype.cache = function (key, data, ttl) {
  this.redis.set(key, JSON.stringify(data));
  this.redis.expire(key, ttl || 60 * 60 * 24);
};

View.prototype.create = function (job, done) {
  debug('createJob...');
  job = job.toJSON();
  var key = this.cacheKey(job.id);
  this.cache(key, job, 60 * 60 * 24);
  done();
};

View.prototype.progress = function (job, done) {
  debug('progress...');
  var key = this.cacheKey(job.id);
  this.cache(key, job, 60 * 60 * 24);
  done();
};

View.prototype.update = function (job, done) {
  var key = this.cacheKey(job.id);
  this.cache(key, job, 60 * 60 * 24);
  done();
};

View.prototype.get = function (id, done) {
  this.redis.get(this.cacheKey(id), function (err, job) {
    if(err) {
      return done(err);
    }
    job = JSON.parse(job);
    done(null, job);
  });
};

View.prototype.count = function (query, done) {
  done(null, 0);
};

View.prototype.find = function (query, options, done) {
  done(null, []);
};

View.prototype.types = function (query, done) {
  done(null, []);
};

module.exports = View;
