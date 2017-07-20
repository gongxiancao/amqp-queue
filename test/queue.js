'use strict';

var Queue = require('../lib/queue'),
  // when = require('when'),
  nodefn = require('when/node'),
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

      nodefn.bindCallback(
        nodefn.call(function (done) {
          setTimeout(done, 1000);
        })
        .then(function () {
          return nodefn.call(function (done) {
            console.log('setp 1...');
            job.progress(2, 10, 'step 1...', done);
          });
        })
        .then(function () {
          return nodefn.call(function (done) {
            setTimeout(done, 200);
          });
        })
        .then(function () {
          return nodefn.call(function (done) {
            console.log('setp 2...');
            job.progress(4, 10, 'step 2...', done);
          });
        })
        .then(function () {
          return nodefn.call(function (done) {
            setTimeout(done, 2000);
          });
        })
        .then(function () {
          return nodefn.call(function (done) {
            console.log('setp 3...');
            job.progress(6, 10, 'step 3...', done);
          });
        })
        .then(function () {
          return nodefn.call(function (done) {
            setTimeout(done, 1000);
          });
        })
        .then(function () {
          return nodefn.call(function (done) {
            console.log('setp 4...');
            job.progress(9, 10, 'step 4...', done);
          });
        }), done);
    });

    var progressSpy = sinon.spy();
    var completeSpy = sinon.spy();

    this.timeout(700000);
    var job = queue.create('testJob', {dataField1: 1});
    job.on('complete', function (data) {
      should.exist(data.id);
      should.equal(data.state, 'complete');
      should.exist(data.completedAt);
      should.exist(data.duration);
      completeSpy();
    });

    job.save(function (err) {
      should.not.exist(err);
      should.exist(job.id);

      queue.get(job.id, function (err, job) {
        job.on('progress', progressSpy);
      });
    });

    setTimeout(function () {
      queue.shutdown(function () {
        should.equal(progressSpy.callCount, 4);
        should.equal(completeSpy.callCount, 1);
        done();
      });
    }, 4000);
  });

  it('should lightweight job get processed', function (done) {
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

      nodefn.bindCallback(nodefn.call(function (done) {
          setTimeout(done, 500);
      })
      .then(function () {
        return nodefn.call(function (done) {
          console.log('setp 1...');
          job.progress(2, 10, 'step 1...', done);
        });
      }), done);
    });

    var progressSpy = sinon.spy();

    this.timeout(10000);
    var job = queue.create('testJob', {dataField1: 1}, {lightweight: true});

    job.save(function (err) {
      should.not.exist(err);
      should.exist(job.id);
      queue.get(job.id, function (err, _job) {
        should.not.exist(_job);
        job.on('progress', function () {
          console.log('on progress');
          progressSpy();
        });
      });
    });

    setTimeout(function () {
      queue.shutdown(function () {
        should.equal(progressSpy.callCount, 0);
        done();
      });
    }, 2000);
  });

it('should failed job get reprocessed and report error once after retry limit', function (done) {
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

    var retry = 3;
    var processSpy = sinon.spy();
    queue.process('testJob', {retry: retry}, function (job, done) {
      console.log('process job ' + job.id);
      processSpy();

      should.equal(job.state, 'active');

      nodefn.bindCallback(nodefn.call(function (done) {
          setTimeout(done, 200);
      })
      .then(function () {
          return when.reject(new Error('failed'));
      }), done);
    });

    var completeSpy = sinon.spy();
    var errorSpy = sinon.spy();

    this.timeout(30000);
    var job = queue.create('testJob', {});

    job.save(function (err) {
      should.not.exist(err);
      should.exist(job.id);
      queue.get(job.id, function (err, _job) {
        should.exist(_job);
        _job.on('complete', function () {
          console.log('on complete');
          completeSpy();
        });
        _job.on('error', function () {
          console.log('on error');
          errorSpy();
        });
      });
    });

    setTimeout(function () {
      queue.shutdown(function () {
        should.equal(processSpy.callCount, retry + 1);
        should.equal(completeSpy.callCount, 0);
        should.equal(errorSpy.callCount, 1);
        done();
      });
    }, 10000);
  });

  it('should failed job get reprocessed and report complete after success retry', function (done) {
    var processedTimes = 0;
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

    var retry = 4;
    var processSpy = sinon.spy();
    queue.process('testJob', {retry: retry}, function (job, done) {
      console.log('process job ' + job.id);
      processSpy();

      should.equal(job.state, 'active');

      nodefn.bindCallback(nodefn.call(function (done) {
          setTimeout(done, 200);
      })
      .then(function () {
        if( ++ processedTimes < 3) {
          return when.reject(new Error('failed'));
        }
      }), done);
    });

    var completeSpy = sinon.spy();
    var errorSpy = sinon.spy();

    this.timeout(30000);
    var job = queue.create('testJob', {});

    job.save(function (err) {
      should.not.exist(err);
      should.exist(job.id);
      queue.get(job.id, function (err, _job) {
        should.exist(_job);
        _job.on('complete', function () {
          console.log('on complete');
          completeSpy();
        });
        _job.on('error', function () {
          console.log('on error');
          errorSpy();
        });
      });
    });

    setTimeout(function () {
      queue.shutdown(function () {
        should.equal(processSpy.callCount, 3);
        should.equal(completeSpy.callCount, 1);
        should.equal(errorSpy.callCount, 0);
        done();
      });
    }, 10000);
  });
});