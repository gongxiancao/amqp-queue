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
  noop: function (){},
  remove: function (array, item) {
    var index = array.indexOf(item);
    if(index >= 0) {
      array.splice(index, 1);
    }
    return array;
  }
};