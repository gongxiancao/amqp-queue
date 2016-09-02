'use strict';

var gulp = require('gulp');
var jshint = require('gulp-jshint');
var mocha = require('gulp-mocha');

gulp.task('jshint', function() {
  return gulp.src(['./lib/*.js', './test/*.js'])
    .pipe(jshint('.jshint'))
    .pipe(jshint.reporter('jshint-stylish'));
});


gulp.task('mocha', function () {
  return gulp.src('test/*.js', {read: false})
    // gulp-mocha needs filepaths so you can't have any plugins before it 
    .pipe(mocha({reporter: 'nyan'}))
    .once('error', function (err) {
      console.error(err.stack);
      process.exit(1);
    })
    .once('end', function () {
      process.exit();
    });
});
