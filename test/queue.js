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
      should.equal(job.data.dataField1, 1);
      async.series([
        function (done) {
          setTimeout(done, 200);
        },
        function (done) {
          job.progress(2, 10, 'step 1...', done);
        },
        function (done) {
          setTimeout(done, 200);
        },
        function (done) {
          job.progress(4, 10, 'step 2...', done);
        },
        function (done) {
          setTimeout(done, 200);
        },
        function (done) {
          job.progress(6, 10, 'step 3...', done);
        },
        function (done) {
          setTimeout(done, 200);
        },
        function (done) {
          job.progress(9, 10, 'step 4...', done);
        }
      ], done);
    });

    var progressSpy = sinon.spy();

    this.timeout(5000);
    var job = queue.create('testJob', {dataField1: 1});
    job.on('complete', function (data) {
      should.exist(data.id);
      should.equal(data.state, 'complete');
      should.exist(data.completedAt);
      should.exist(data.duration);
      should.equal(progressSpy.callCount, 4);
      done();
    });

    job.save(function (err) {console.log(err);
      should.not.exist(err);
      should.exist(job.id);

      queue.find({id: job.id}, {}, function (err, jobs) {
        should.not.exist(err);
        should.equal(jobs.length, 1);
      });
      queue.count({id: job.id}, function (err, count) {
        should.not.exist(err);
        should.equal(count, 1);
      });
      queue.get(job.id, function (err, job) {
        job.on('progress', progressSpy);
      });
    });
  });
});