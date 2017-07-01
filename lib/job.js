'use strict';
var uuid = require('uuid'),
  debug = require('debug')('amqp-queue:job'),
  _util = require('./util'),
  util = require('util'),
  when = require('when'),
  nodefn = require('when/node'),
  EventEmitter = require('events').EventEmitter;

function Job(type, queue, data, options) {
  if(!(this instanceof Job)) {
    return new Job(type, queue, data);
  }

  EventEmitter.call(this);
  this.type = type;
  this.data = data;
  this._channel = null;
  this._msg = null;
  this._acked = false;
  this.properties = null;
  this.options = options || {};
  this.monitorQueues = {};

  Object.defineProperty(this, 'queue', {enumerable: false, value: queue});
}

util.inherits(Job, EventEmitter);

Job.prototype.supportMonitor = function () {
  return !this.options.lightweight;
};

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
    nodefn.bindCallback(when.all([
      nodefn.call(function (done) {
        if(!self.supportMonitor()) {
          return done();
        }
        self.queue.view.create(self, function () {
          done();
        });
      }),
      nodefn.call(function (done) {
        self.queue.channel.publish(self.queue.exchange, self.type, new Buffer(JSON.stringify(self.toJSON())));
        done();
      })
    ]), function () {
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
    options: this.options,
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
  var self = this;
  done = done || _util.noop;
  if(!self.supportMonitor()) {
    return done();
  }

  this._progress = Math.floor(100 * frames / totalFrames);
  this.message = message;

  this.queue.assertSetup(function (err) {
    debug('progress assertSetup done', err);
    if(err) {
      return done(err);
    }
    var progress = {evt: 'progress', status: self.toJSON()};
    debug('progress publish %s progress.%s frames/totalFrames=%d/%d message=%s', self.queue.exchange, self.id, frames, totalFrames, message);
    self.queue.channel.publish(self.queue.exchange, 'res.' + self.type, new Buffer(JSON.stringify(progress)));
    self.queue.view.progress(self, done);
  });
};

var on = Job.prototype.on;
Job.prototype.on = function (evt) {
  var self = this;
  if(!self.supportMonitor()) {
    return;
  }
  on.apply(self, Array.prototype.slice.call(arguments, 0));
  self._refreshMonitorQueue(function (err) {
    if(err) {
      return self.emit('error', err);
    }
    self.queue.consumeJobEvent(self, evt, function (msg) {
      self.emit(evt, msg);
    });
  });
};

Job.prototype._refreshMonitorQueue = function (done) {
  done = done || _util.noop;
  var self = this;
  self.queue.assertSetup(function (err) {
    if(err) {
      return self.emit('error', err);
    }
    self.queue.assertResponseQueue(done);
  });
};

Job.prototype.ack = function () {
  if(this._channel && !this._acked) {
    this._acked = true;
    var args = [this._msg].concat(Array.prototype.slice.call(arguments, 0));
    this._channel.ack.apply(this._channel, args);
  }
};

Job.prototype.nack = function () {
  if(this._channel && !this._acked) {
    this._acked = true;
    var args = [this._msg].concat(Array.prototype.slice.call(arguments, 0));
    this._channel.nack.apply(this._channel, args);
  }
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
      if(err || !job) {
        return done(err);
      }
      job = Job.fromJSON(queue, job);
      done(null, job);
    });
  });
};



module.exports = Job;
