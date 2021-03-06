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
    return new Queue(name, options);
  }
  this.id = uuid.v4();
  this._options = options;
  this.options = {
    prefix: (options.prefix || '') + 'amqp-queue:',
    amqp: options.amqp
  };
  this.pendingJobs = [];
  this.options.view = _util.extend({}, options.view, {prefix: this.options.prefix});

  this.lockKey = this.options.prefix + uuid.v4();
  this.name = name;

  this.queues = {};

  this.jobEventHandlers = [];
  this.exchange = this.options.prefix + name;
  this.deadLetterExchange = this.options.prefix + name + '.deadLetter';
  this.processesRegistered = [];
  this.consumerTags = [];

  this.responseQueueName = this.exchange + '.res.' + uuid.v4();
  this.responseQueue = null;
  this.responseQueueConsumerBound = false;

  this.responseQueueBoundJobTypes = {};
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
            ch.assertExchange(self.deadLetterExchange, 'topic', {durable: true});
            debug('got channel, releasing lock');
            self.assertDeadLetterQueue(function (err) {
              release()();
              done(err);
            });
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

Queue.prototype.assertResponseQueue = function (done) {
  var self = this;
  if(self.responseQueue) {
    return done(null, self.responseQueue);
  }
  self.channel.assertQueue(self.responseQueueName, {exclusive: true}, function (err, q) {
    self.responseQueue = q;

    if(self.responseQueueConsumerBound) {
      return done(null, q);
    }
    self.responseQueueConsumerBound = true;
    self.channel.consume(self.responseQueue.queue, function (msg) {
      msg = JSON.parse(msg.content.toString());
      self.jobEventHandlers.forEach(function (handler) {
        if(handler.job.id === msg.status.id && handler.evt === msg.evt) {
          handler.handler.call(handler.job, msg.status);
        }
      });
      if(msg.evt === 'complete' || msg.evt === 'error') {
        self.unconsumeJobEvent(msg.status);
      }
    }, {noAck: true});

    done(err, q);
  });
};

Queue.prototype.consumeJobEvent = function (job, evt, handler) {
  var self = this;
  self.jobEventHandlers.push({job: job, evt: evt, handler: handler});

  if(self.responseQueueBoundJobTypes[job.type]) {
    return;
  }

  debug('bindQueue: %s, %s, %s', self.responseQueue.queue, self.exchange, job.type);
  self.channel.bindQueue(self.responseQueue.queue, self.exchange, 'res.' + job.type);
  self.responseQueueBoundJobTypes[job.type] = true;
};

Queue.prototype.unconsumeJobEvent = function (job) {
  var self = this;
  var jobEventHandlers = [];
  self.jobEventHandlers.forEach(function (handler) {
    if(!(handler.job === job || handler.job.id === job.id)) {
      jobEventHandlers.push(handler);
    }
  });
  self.jobEventHandlers = jobEventHandlers;
};

Queue.prototype.assertDeadLetterQueue = function (done) {
  var self = this;
  if(self.deadLetterQueue) {
    return done(null, self.deadLetterQueue);
  }
  debug('assert dead letter queue');
  self.deadLetterQueue = self.deadLetterExchange + ':retry_message';
  self.channel.assertQueue(self.deadLetterQueue, {arguments: {'x-message-ttl': self.options.retryTTL || 3000, 'x-dead-letter-exchange': self.exchange}}, function (err, q) {
    if(err) {
      return done(err);
    }
    self.channel.bindQueue(q.queue, self.deadLetterExchange, '*');
    done(err, q);
  });
};

Queue.prototype.create = function (type, data, options) {
  return new Job(type, this, data, options);
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

Queue.prototype.assertQueue = function (type, done) {
  var self = this;
  if(self.queues[type]) {
    return done(null, self.queues[type]);
  }
  self.channel.assertQueue(self.exchange + ':' + type, {arguments: {'x-dead-letter-exchange': self.deadLetterExchange}}, function (err, q) {
    if(err) {
      return done(err);
    }
    self.queues[type] = q;
    self.channel.bindQueue(q.queue, self.exchange, type, {}, function (err) {
      done(err, self.queues[type]);
    });
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
  function handleJobError(err, job, msg) {
    if(!job.supportMonitor()) {
      return;
    }
    job.state = 'failed';
    job.error = err.toString();
    job.failedAt = new Date();
    job.duration = job.failedAt - job.createdAt;

    var xDeth = msg.properties.headers['x-death'] || [];
    var deadLetterExchangeDeth = xDeth.find(function (deth) {
      return deth.exchange === self.deadLetterExchange;
    });
    deadLetterExchangeDeth = deadLetterExchangeDeth || {count: 0};
    debug('deadLetterExchangeDeth:%s', deadLetterExchangeDeth);
    if(!options.noAck && deadLetterExchangeDeth.count < options.retry) {
      job.nack(false, false); // will retry using dead letter queue
    } else {
      self.channel.publish(self.exchange, 'res.' + job.type, new Buffer(JSON.stringify({evt: 'error', status: job.toJSON()})));
      self.view.update(job, handleViewError);
      if(!options.noAck) {
        job.ack();
      }
    }
  }

  debug('process begin assertSetup');
  self.assertSetup(function (err) {
    debug('process end assertSetup');
    if(err) {
      return handleQueueError(err);
    }

    self.assertQueue(type, function (err, q) {
      debug('consume...');
      if(err) {
        return handleQueueError(err);
      }
      if(options.prefetch) {
        self.channel.prefetch(options.prefetch);
      }

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
          if(!job.supportMonitor()) {
            return done();
          }
          self.view.update(job, function (err) {
            handleViewError(err);
            done(err);
          });
        })
        .then(function () {
          return nodefn.call(function (done) {
            debug('handle job...');
            handler(job, function (err) {
              function drain() {
                _util.remove(self.pendingJobs, job.id);
                if(!self.pendingJobs.length) {
                  self.emit('drain');
                }
              }

              if(err) {
                drain();
                return handleJobError(err, job, msg);
              }
              if(!options.noAck) {
                job.ack();
              }
              debug('job complete: %s complete.%s', self.exchange, job.id);

              if(!job.supportMonitor()) {
                drain();
                return done();
              }

              job.state = 'complete';
              job.completedAt = new Date();
              job.duration = job.completedAt - job.createdAt;

              self.channel.publish(self.exchange, 'res.' + job.type, new Buffer(JSON.stringify({evt: 'complete', status: job.toJSON()})));
              self.view.update(job, function (err) {
                handleViewError(err);
                drain();
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
  nodefn.bindCallback(when.all(self.processesRegistered).then(function () {
    var cancel = nodefn.lift(self.channel.cancel);
    return when.map(self.consumerTags, function (consumerTag) {
      return cancel.call(self.channel, consumerTag);
    });
  })
  .then(function () {
    return when.resolve().delay(1000);
  })
  .then(function () {
    return nodefn.call(function (done) {
      self.removeAllListeners();
      if(!self.pendingJobs.length) {
        return done();
      }
      self.on('drain', function () {
        self.amqpConnection.close(done);
      });
    });
  }), done);

};

module.exports = Queue;
