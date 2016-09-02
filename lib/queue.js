'use strict';
var Job = require('./job'),
  View = require('./view'),
  amqp = require('amqplib/callback_api'),
  uuid = require('uuid'),
  lock = require('lock')(),
  debug = require('debug')('amqp-queue:queue'),
  util = require('util'),
  _util = require('./util'),
  async = require('async'),
  EventEmitter = require('events').EventEmitter;

function Queue (name, options) {
  if(!(this instanceof Queue)) {
    return new Queue(options);
  }
  this.options = {
    prefix: (options.prefix || '') + 'amqp-queue:',
    amqp: options.amqp
  };

  this.options.view = _util.extend({}, options.view, {prefix: this.options.prefix});

  this.lockKey = this.options.prefix + uuid.v4();
  this.name = name;
  this.exchange = this.options.prefix + name;
}

util.inherits(Queue, EventEmitter);

Queue.prototype.assertSetup = function (done) {
  var self = this;
  async.parallel([
    function (done) {
      if(!self.channel) {
        debug('locking channel', self.lockKey);
        return lock(self.lockKey + ':channel', function (release) {
          debug('channel lock acquired', self.lockKey);
          if(self.channel) {
            debug('channel already created, return.', self.lockKey);
            release()();
            return done();
          }
          debug('connecting amqp ...', self.lockKey);
          amqp.connect('amqp://' + self.options.amqp.host, function (err, conn) {
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
      }
      done();
    },
    function (done) {
      if(!self.view) {
        self.view = new View(self.name, self.options.view);
      }
      done();
    }
  ], done);
};

Queue.prototype.create = function (type/*, data*/) {
  return new Job(type, this);
};

Queue.prototype.get = function (id, done) {
  Job.get(this, id, done);
};

Queue.prototype.process = function (type, handler) {
  var self = this;
  var job, queue;
  async.series([
    function (done) {
      self.assertSetup(done);
    },
    function (done) {
      self.channel.assertQueue(self.exchange + ':' + type, {}, function (err, q) {
        if(err) {
          return done(err);
        }
        queue = q.queue;
        done();
      });
    },
    function (done) {
      debug('cosume...');
      self.channel.bindQueue(queue, self.exchange, type);
      self.channel.consume(queue, function (msg) {
        debug('receive job %s:\'%s\'', msg.fields.routingKey, msg.content.toString());
        job = JSON.parse(msg.content);
        job = Job.fromJSON(self, job);
        done();
      }, {noAck: true});
    },
    function (done) {
      debug('update job state to active...');
      job.state = 'active';
      self.view.update(job, done);
    },
    function (done) {
      debug('handle job...');
      handler(job, done);
    }
  ], function (err) {
    if(err) {
      if(!job) {
        return self.emit('error', err.toString());
      }
      job.state = 'error';
      job.message = err.toString();
      job.failedAt = new Date();
      job.duration = job.failedAt - job.createdAt;
      self.channel.publish(self.exchange, 'error.' + job.id, new Buffer(JSON.stringify(job.toJSON())));
    } else {
      debug('job complete: %s complete.%s', self.exchange, job.id);
      job.state = 'complete';
      job.completedAt = new Date();
      job.duration = job.completedAt - job.createdAt;
      self.channel.publish(self.exchange, 'complete.' + job.id, new Buffer(JSON.stringify(job.toJSON())));
    }
    if(job) {
      self.view.update(job, function (err) {
        if(err) {
          return self.emit('error', err.toString());
        }
      });      
    }
  });
};

module.exports = Queue;
