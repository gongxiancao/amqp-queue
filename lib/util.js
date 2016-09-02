'use strict';
module.exports = {
  extend: function (target) {
    var source;
    for(var i = 1, l = arguments.length; i < l; ++i) {
      source = arguments[i];
      for(var key in source) {
        target[key] = source[key];
      }
    }
    return target;
  },
  noop: function (){}
};