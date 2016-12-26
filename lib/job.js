'use strict';
var uuid = require('uuid'),
  debug = require('debug')('amqp-queue:job'),
  _util = require('./util'),
  util = require('util'),
  EventEmitter = require('events').EventEmitter;

function Job(type, queue, data) {
  if(!(this instanceof Job)) {
    return new Job(type, queue, data);
  }
  EventEmitter.call(this);
  this.type = type;
  this.data = data;
  this.monitorQueues = {};
  Object.defineProperty(this, 'queue', {enumerable: false, value: queue});
}

util.inherits(Job, EventEmitter);

Job.prototype.save = function (done) {
  var self = this;
  done = done || _util.noop;
  if(this.id) {
    return done();
  }
  this.queue.assertSetup(function (err) {
    debug('assertSetup done');
    if(err) {
      return done(err);
    }
    self.id = uuid.v4();
    self.createdAt = new Date();
    self.state = 'inactive';

    debug('publishing job: ', self.queue.exchange, self.type);
    self._refreshMonitorQueue();
    self.queue.view.create(self, function () {
      self.queue.channel.publish(self.queue.exchange, self.type, new Buffer(JSON.stringify(self.toJSON())));
      done(null, self);
    });
  });
};

Job.prototype.toJSON = function () {
  return {
    id: this.id,
    type: this.type,
    createdAt: this.createdAt || null,
    failedAt: this.failedAt || null,
    completedAt: this.completedAt || null,
    duration: this.duration || null,
    state: this.state,
    progress: this._progress || null,
    error: this.error,
    message: this.message,
    data: this.data
  };
};

Job.prototype.fromJSON = function (value) {
  value._progress = value.progress;
  delete value.progress;
  _util.extend(this, value);

  if(this.createdAt) {
    this.createdAt = new Date(this.createdAt);
  }
  if(this.failedAt) {
    this.failedAt = new Date(this.failedAt);
  }
  if(this.completedAt) {
    this.completedAt = new Date(this.completedAt);
  }

  return this;
};

Job.prototype.progress = function (frames, totalFrames, message, done) {
  this._progress = Math.floor(100 * frames / totalFrames);
  this.message = message;
  done = done || _util.noop;
  var self = this;
  this.queue.assertSetup(function (err) {
    debug('progress assertSetup done', err);
    if(err) {
      return done(err);
    }
    debug('progress publish %s progress.%s frames/totalFrames=%d/%d message=%s', self.queue.exchange, self.id, frames, totalFrames, message);
    self.queue.channel.publish(self.queue.exchange, 'progress.' + self.id, new Buffer(JSON.stringify(self.toJSON())));
    self.queue.view.progress(self, done);
  });
};

var on = Job.prototype.on;
Job.prototype.on = function () {
  on.apply(this, Array.prototype.slice.call(arguments, 0));
  this._refreshMonitorQueue();
};

var eventsToMonitor = ['error', 'progress', 'complete'];
Job.prototype._refreshMonitorQueue = function () {
  var self = this;
  if(!this.id) {
    return;
  }
  self.queue.assertSetup(function (err) {
    if(err) {
      return self.emit('error', err);
    }
    eventsToMonitor.forEach(function (evt) {
      if(self.listeners(evt).length) {
        if(!self.monitorQueues[evt]) {
          self.monitorQueues[evt] = self.queue.exchange + ':'+ evt + ':' + uuid.v4();
          debug('assertQueue', self.monitorQueues[evt]);
          self.queue.channel.assertQueue(self.monitorQueues[evt], {exclusive: true}, function (err, q) {
            if(err) {
              return self.emit('error', err);
            }
            debug('bindQueue: %s, %s, %s.%s', q.queue, self.queue.exchange, evt, self.id);
            self.queue.channel.bindQueue(q.queue, self.queue.exchange, evt + '.' + self.id);
            self.queue.channel.consume(q.queue, function (msg) {
              msg = JSON.parse(msg.content.toString());
              self.fromJSON(msg);
              debug('emitting %s %s', evt, msg.id);
              self.emit(evt, msg);
            }, {noAck: true});
          });
        }
      }
    });
  });
};

Job.fromJSON = function (queue, value) {
  var job = new Job(value.type, queue);
  return job.fromJSON(value);
};

Job.get = function (queue, id, done) {

  queue.assertSetup(function (err) {
    if(err) {
      return done(err);
    }
    queue.view.get(id, function (err, job) {
      if(err) {
        return done(err);
      }
      job = Job.fromJSON(queue, job);
      done(null, job);
    });
  });
};

module.exports = Job;
