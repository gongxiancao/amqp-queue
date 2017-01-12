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
  createdAt: { type: Date, expires: '30d'},
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

  mongoose.Promise = this.options.Promise || mongoose.Promise;
  this.connection = mongoose.createConnection(uri, { config: { autoIndex: true, promiseLibrary: mongoose.Promise } });

  var jobSchema = new Schema(jobAttributes, {collection: this.jobCollectionName});
  this.Job = this.connection.model('Job', jobSchema);

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
  this.Job.create(job, done);
};

View.prototype.progress = function (job, done) {
  debug('progress...');
  var key = this.cacheKey(job.id);
  this.cache(key, job, 60 * 60 * 24);
  // this.Job.update({id: job.id}, job, done);
  done();
};

View.prototype.update = function (job, done) {
  var key = this.cacheKey(job.id);
  this.cache(key, job, 60 * 60 * 24);
  var state = job.state;
  var self = this;

  this.Job.update({id: job.id}, {$set: job.toJSON()}, done);
};

View.prototype.get = function (id, done) {
  var self = this;
  this.redis.get(this.cacheKey(id), function (err, job) {
    if(err) {
      return done(err);
    }
    if(!job) {
      return self.Job.findOne({id: id})
        .lean()
        .exec(function (err, job) {
          if(err) {
            return done(err);
          }
          done(null, job);
          if(job) {
            self.cache(self.cacheKey(job.id), job, 60 * 5);
          }
        });
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
