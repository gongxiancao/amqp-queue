'use strict';

var Queue = require('../lib/queue');
console.log('starting ...');
var should = require('chai').should();
/* globals
  describe: false,
  it: false
  */

describe('queue', function() {
  it('should job get saved', function (done) {
    var queue = new Queue('test', {
      amqp: {host: 'localhost'},
      redis: {
        host: 'localhost',
        port: 6379,
        prefix: 'test'
      }
    });

    queue.process('testJob', function (job, done) {
      setTimeout(function () {
      job.progress(0.1, 'step1...');
      job.progress(0.5, 'step2...');
      done();
      }, 100);
      // done(null, {});
    });
    this.timeout(5000);
    var job = queue.create('testJob', {});

    job.save(function (err) {
      job.on('complete', function (data) {
        should.exist(data.id);
        done();
      });

      should.not.exist(err);
      should.exist(job.id);
      queue.get(job.id, function (err, job) {
        job.on('progress', function () {
          console.log('progress...', arguments);
        });
      });
    });
  });
});