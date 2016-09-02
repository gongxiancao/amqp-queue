'use strict';

var Queue = require('../lib/queue'),
  async = require('async'),
  should = require('chai').should(),
  sinon = require('sinon');

/* globals
  describe: false,
  it: false
  */

describe('queue', function() {
  it('should job get processed', function (done) {
    var queue = new Queue('test', {
      amqp: {host: 'localhost'},
      view: {
        redis: {
          host: 'localhost',
          port: 6379,
        },
        mongo: {
          hosts: [{ host: 'localhost', port: 27017}],
          database: 'test'
        }
      }
    });

    queue.process('testJob', function (job, done) {
      should.equal(job.state, 'active');
      console.log('in processing...');
      async.series([
        function (done) {
          setTimeout(done, 200);
        },
        function (done) {
          job.progress(0.2, 'step 1...', done);
        },
        function (done) {
          setTimeout(done, 200);
        },
        function (done) {
          job.progress(0.4, 'step 2...', done);
        },
        function (done) {
          setTimeout(done, 200);
        },
        function (done) {
          job.progress(0.6, 'step 3...', done);
        },
        function (done) {
          setTimeout(done, 200);
        },
        function (done) {
          job.progress(0.9, 'step 4...', done);
        }
      ], done);
    });

    var progressSpy = sinon.spy();

    this.timeout(5000);
    var job = queue.create('testJob', {});
    job.on('complete', function (data) {
      should.exist(data.id);
      should.equal(data.state, 'complete');
      should.exist(data.completedAt);
      should.exist(data.duration);
      should.equal(progressSpy.callCount, 4);
      done();
    });

    job.save(function (err) {
      should.not.exist(err);
      should.exist(job.id);
      queue.get(job.id, function (err, job) {
        job.on('progress', progressSpy);
      });
    });
  });
});