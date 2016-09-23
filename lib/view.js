'use strict';
var mongoose = require('mongoose'),
  mongodbUri = require('mongodb-uri'),
  debug = require('debug')('amqp-queue:view'),
  redis = require('redis'),
  async = require('async'),
  Schema = mongoose.Schema;

var jobAttributes = {
  id: {type: String, unique: true},
  type: { type: String, index: true},
  state: { type: String, index: true},
  createdAt: { type: Date, index: true},
  completedAt: { type: Date, expires: '7d'},
  failedAt: {type: Date, expires: '30d'},
  duration: Number,
  progress: Number,
  data: Schema.Types.Mixed,
  message: String,
  error: String
};

function View (name, options) {
  if(!(this instanceof View)) {
    return new View(options);
  }
  this.options = options;
  this.jobCollectionName = options.prefix + name + ':jobs';
  var uri = mongodbUri.format(options.mongo);
  this.connection = mongoose.createConnection(uri, { config: { autoIndex: true } });

  var jobSchema = new Schema(jobAttributes, {collection: this.jobCollectionName});
  this.Job = this.connection.model('Job', jobSchema);

  this.redis = redis.createClient({host: options.redis.host, port: options.redis.port || 6379});
}
View.prototype.cacheKey = function (id) {
  return this.jobCollectionName + ':' + id;
};

View.prototype.create = function (job, done) {
  debug('createJob...');
  job = job.toJSON();
  var key = this.cacheKey(job.id);
  this.redis.set(key, JSON.stringify(job));
  this.redis.expire(key, 60 * 60 * 24 * 7);
  this.Job.create(job, done);
};

View.prototype.progress = function (job, done) {
  debug('progress...');
  var key = this.cacheKey(job.id);
  this.redis.set(key, JSON.stringify(job.toJSON()));
  this.redis.expire(key, 60 * 60 * 24 * 7);
  // this.Job.update({id: job.id}, job, done);
  done();
};

View.prototype.update = function (job, done) {
  var key = this.cacheKey(job.id);
  this.redis.set(key, JSON.stringify(job));
  this.redis.expire(key, 60 * 60 * 24 * 7);
  this.Job.update({id: job.id}, job.toJSON(), done);
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
  this.Job.count(query, done);
};

View.prototype.find = function (query, options, done) {
  var self = this;
  this.Job.find(query)
    .select('id')
    .skip(options.skip || 0)
    .limit(options.limit || 10)
    .exec(function (err, jobs) {
      if(err) {
        return done(err);
      }
      async.map(jobs, function (job, done) {
        self.get(job.id, done);
      }, done);
    });
};

View.prototype.types = function (query, done) {
  this.Job.find(query)
    .distinct('type')
    .exec(done);
};

module.exports = View;
