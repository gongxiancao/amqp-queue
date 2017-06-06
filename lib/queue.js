'use strict';
var Job = require('./job'),
  View = require('./view'),
  amqp = require('amqplib/callback_api'),
  uuid = require('uuid'),
  lock = require('lock')(),
  debug = require('debug')('amqp-queue:queue'),
  util = require('util'),
  _util = require('./util'),
  when = require('when'),
  nodefn = require('when/node'),
  EventEmitter = require('events').EventEmitter;

function Queue (name, options) {
  if(!(this instanceof Queue)) {
    return new Queue(options);
  }
  this.options = {
    prefix: (options.prefix || '') + 'amqp-queue:',
    amqp: options.amqp
  };
  this.pendingJobs = [];
  this.options.view = _util.extend({}, options.view, {prefix: this.options.prefix});

  this.lockKey = this.options.prefix + uuid.v4();
  this.name = name;
  this.exchange = this.options.prefix + name;
  this.processesRegistered = [];
  this.consumerTags = [];
}

util.inherits(Queue, EventEmitter);

function amqpUrl(options) {
  return 'amqp://' + (options.username? (options.username + ':' + options.password + '@'):'') + options.host;
}

Queue.prototype.assertSetup = function (done) {
  var self = this;
  nodefn.bindCallback(when.all([
    nodefn.call(function (done) {
      if(self.channel) {
        return done();
      }

      debug('locking channel', self.lockKey);
      return lock(self.lockKey + ':channel', function (release) {
        debug('channel lock acquired', self.lockKey);
        if(self.channel) {
          debug('channel already created, return.', self.lockKey);
          release()();
          return done();
        }
        debug('connecting amqp ...', self.lockKey);
        amqp.connect(amqpUrl(self.options.amqp), function (err, conn) {
          debug('amqp connected', self.lockKey);
          if(err) {
            release()();
            return done(err);
          }
          self.amqpConnection = conn;
          conn.createChannel(function (err, ch) {
            if(err) {
              release()();
              return done(err);
            }
            self.channel = ch;
            ch.assertExchange(self.exchange, 'topic', {durable: true});
            debug('got channel, releasing lock');
            release()();
            done();
          });
        });
      });
    }),
    nodefn.call(function (done) {
      if(!self.view) {
        self.view = new View(self.name, self.options.view);
      }
      done();
    })
  ]), done);
};

Queue.prototype.create = function (type, data) {
  return new Job(type, this, data);
};

Queue.prototype.get = function (id, done) {
  Job.get(this, id, done);
};

Queue.prototype.count = function (query, done) {
  var self = this;
  self.assertSetup(function (err) {
    if(err) {
      return done(err);
    }
    self.view.count(query, done);
  });
};

Queue.prototype.find = function (query, options, done) {
  var self = this;
  self.assertSetup(function (err) {
    if(err) {
      return done(err);
    }
    self.view.find(query, options, done);
  });
};

Queue.prototype.types = function (query, done) {
  var self = this;
  self.assertSetup(function (err) {
    if(err) {
      return done(err);
    }
    self.view.types(query, done);
  });
};

Queue.prototype.process = function (type, options, handler) {
  var self = this;
  if(typeof options === 'function') {
    handler = options;
    options = {};
  }

  if(self.shuttingDown) {
    return;
  }

  var processRegistered = when.defer();
  self.processesRegistered.push(processRegistered.promise);

  function handleQueueError(err) {
    return self.emit('error', err.toString());
  }

  function handleViewError(err) {
    if(err) {
      return self.emit('error', err.toString());
    }
  }
  function handleJobError(err, job) {
    job.state = 'failed';
    job.error = err.toString();
    job.failedAt = new Date();
    job.duration = job.failedAt - job.createdAt;
    self.channel.publish(self.exchange, 'error.' + job.id, new Buffer(JSON.stringify(job.toJSON())));
    self.view.update(job, handleViewError);
    if(!options.noAck) {
      job.nack(true, false); // no requeue by default for compatibility
    }
  }
  debug('process begin assertSetup');
  self.assertSetup(function (err) {
    debug('process end assertSetup');
    if(err) {
      return handleQueueError(err);
    }

    self.channel.assertQueue(self.exchange + ':' + type, {}, function (err, q) {
      if(err) {
        return handleQueueError(err);
      }

      debug('consume...');
      if(options.prefetch) {
        self.channel.prefetch(options.prefetch);
      }
      self.channel.bindQueue(q.queue, self.exchange, type);
      self.channel.consume(q.queue, function (msg) {
        debug('receive job %s:\'%s\'', msg.fields.routingKey, msg.content.toString());
        var job = JSON.parse(msg.content);
        job = Job.fromJSON(self, job);

        job._channel = self.channel;
        job._msg = msg;
        job.properties = msg.properties;

        self.pendingJobs.push(job.id);

        debug('update job state to active...');
        job.state = 'active';

        nodefn.call(function (done) {
          self.view.update(job, function (err) {
            handleViewError(err);
            done(err);
          });
        })
        .then(function () {
          return nodefn.call(function (done) {
            debug('handle job...');
            handler(job, function (err) {
              if(err) {
                return handleJobError(err, job);
              }
              if(!options.noAck) {
                job.ack();
              }
              debug('job complete: %s complete.%s', self.exchange, job.id);

              job.state = 'complete';
              job.completedAt = new Date();
              job.duration = job.completedAt - job.createdAt;
              self.channel.publish(self.exchange, 'complete.' + job.id, new Buffer(JSON.stringify(job.toJSON())));

              self.view.update(job, function (err) {
                handleViewError(err);
                _util.remove(self.pendingJobs, job.id);
                if(!self.pendingJobs.length) {
                  self.emit('drain');
                }
                done();
              });
            });
          });
        });
      }, options, function (err, consume) {
        self.consumerTags.push(consume.consumerTag);
        processRegistered.resolve();
      });
    });
  });
};

Queue.prototype.shutdown = function (done) {
  var self = this;
  if(self.shuttingDown) {
    return done(new Error('Shutdown is in process'));
  }
  self.shuttingDown = true;
  if(!self.processesRegistered.length) {
    return done();
  }
  when.all(self.processesRegistered).then(function () {
    var cancel = nodefn.lift(self.channel.cancel);
    return when.map(self.consumerTags, function (consumerTag) {
      return cancel.call(self.channel, consumerTag);
    });
  })
  .then(function () {
    return when.resolve().delay(1000);
  })
  .then(function () {
    self.removeAllListeners();
    if(!self.pendingJobs.length) {
      return done();
    }
    self.on('drain', function () {
      self.amqpConnection.close(done);
    });
  })
  .catch(done);
};

module.exports = Queue;
