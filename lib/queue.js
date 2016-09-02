'use strict';
var Job = require('./job'),
  amqp = require('amqplib/callback_api'),
  redis = require('redis'),
  uuid = require('uuid'),
  lock = require('lock')(),
  debug = require('debug')('amqp-queue:queue'),
  util = require('util'),
  EventEmitter = require('events').EventEmitter;


function Queue (name, options) {
  if(!(this instanceof Queue)) {
    return new Queue(options);
  }
  this.options = {
    prefix: options.prefix || 'amqp-queue:',
    amqp: options.amqp,
    redis: options.redis
  };

  this.lockKey = this.options.prefix + uuid.v4();
  this.name = name;
  this.exchange = this.options.prefix + name;
}

util.inherits(Queue, EventEmitter);

Queue.prototype.assertSetup = function (done) {
  if(!this.redis) {
    this.redis = redis.createClient({host: this.options.redis.host, port: this.options.redis.port || 6379});
  }
  var self = this;
  if(!this.channel) {
    debug('locking channel', self.lockKey);
    return lock(self.lockKey + ':channel', function (release) {
      debug('lock acquired', self.lockKey);
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
};

Queue.prototype.create = function (type/*, data*/) {
  return new Job(type, this);
};

Queue.prototype.get = function (id, done) {
  Job.get(this, id, done);
};

Queue.prototype.process = function (type, handler) {
  var self = this;
  // throw new Error('not implemented', handler);
  this.assertSetup(function (err) {
    if(err) {
      return self.emit('error', err);
    }
    self.channel.assertQueue(self.exchange + ':' + type, {}, function (err, q) {
      if(err) {
        return self.emit('error', err);
      }
      debug('process ... bind queue ...', q.queue, self.exchange, type);

      self.channel.bindQueue(q.queue, self.exchange, type);
      self.channel.consume(q.queue, function (msg) {
        debug("receive job %s:'%s'", msg.fields.routingKey, msg.content.toString());
        var job = JSON.parse(msg.content);
        job = Job.fromJSON(self, job);
        handler(job, function (err) {
          if(err) {
            job.state = 'error';
            job.message = err.toString();
            return self.channel.publish(self.exchange, 'error.' + job.id, new Buffer(JSON.stringify(job.toJSON())));
          }
          debug('job complete: %s complete.%s', self.exchange, job.id);
          self.channel.publish(self.exchange, 'complete.' + job.id, new Buffer(JSON.stringify(job.toJSON())));
        });
      }, {noAck: true});
    });
  });
};

module.exports = Queue;
