'use strict';
var uuid = require('uuid'),
  debug = require('debug')('amqp-queue:job'),
  _util = require('./util'),
  util = require('util'),
  EventEmitter = require('events').EventEmitter;

function Job(type, queue) {
  if(!(this instanceof Job)) {
    return new Job(queue);
  }
  EventEmitter.call(this);
  this.type = type;
  this.monitorQueues = {};
  Object.defineProperty(this, '_queue', {enumerable: false, value: queue});
}

util.inherits(Job, EventEmitter);

Job.prototype.save = function (done) {
  var self = this;
  if(this.id) {
    return done();
  }
  this._queue.assertSetup(function (err) {
    debug('assertSetup done');
    if(err) {
      return done(err);
    }
    self.id = uuid.v4();
    self.createdAt = new Date();
    self.state = 'inactive';
    debug('publishing job: ', self._queue.exchange, self.type);
    self._queue.channel.publish(self._queue.exchange, self.type, new Buffer(JSON.stringify(self.toJSON())));
    self._queue.redis.set(self._queue.exchange + ':jobs:' + self.id, JSON.stringify(self));
    self._queue.redis.set(self._queue.exchange + ':jobs:' + self.type + ':' + self.id, '');
    done();
  });
};

Job.prototype.toJSON = function () {
  return {
    id: this.id,
    type: this.type,
    createdAt: this.createdAt,
    state: this.state,
    progress: this._progress,
    message: this.message,
    data: this.data
  };
};

Job.prototype.fromJSON = function (value) {
  value._progress = value.progress;
  delete value.progress;
  return _util.extend(this, value);
};

Job.prototype.progress = function (progress, message, done) {
  this._progress = progress;
  this.message = message;
  done = done || _util.noop;
  var self = this;
  this._queue.assertSetup(function (err) {
    debug('progress assertSetup done', err);
    if(err) {
      return done(err);
    }
    debug('progress publish %s progress.%s', self._queue.exchange, self.id);
    self._queue.channel.publish(self._queue.exchange, 'progress.' + self.id, new Buffer(JSON.stringify({progress: progress, message: message})));
    self._queue.redis.set(self._queue.exchange + ':jobs:' + self.id, JSON.stringify(self.toJSON()));
    done();
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
  self._queue.assertSetup(function (err) {
    if(err) {
      return self.emit('error', err);
    }
    eventsToMonitor.forEach(function (evt) {
      if(self.listeners(evt).length) {
        if(!self.monitorQueues[evt]) {
          self.monitorQueues[evt] = self._queue.exchange + ':'+ evt + ':' + uuid.v4();
          debug('assertQueue', self.monitorQueues[evt]);
          self._queue.channel.assertQueue(self.monitorQueues[evt], {exclusive: true}, function (err, q) {
            if(err) {
              return self.emit('error', err);
            }
            debug('bindQueue: %s, %s, %s.%s', q.queue, self._queue.exchange, evt, self.id);
            self._queue.channel.bindQueue(q.queue, self._queue.exchange, evt + '.' + self.id);
            self._queue.channel.consume(q.queue, function (msg) {
              msg = JSON.parse(msg.content.toString());
              debug('emitting %s', evt, msg);
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

    queue.redis.get(queue.exchange + ':jobs:' + id, function (err, value) {
      if(err) {
        return done(err);
      }
      value = JSON.parse(value);
      var job = Job.fromJSON(queue, value);
      done(null, job);
    });
  });
};

module.exports = Job;
