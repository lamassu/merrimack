'use strict';

var P = require('bluebird');

var fdb = require('fdb').apiVersion(300);
var db = fdb.open();
var env = process.env.NODE_ENV || 'development';

exports.init = function init() {
  return fdb.directory.createOrOpen(db, ['merrimack', env])
  .then(function(merrimack) {
    return P.props({
      merrimack: merrimack,
      topics: merrimack.createOrOpen(db, 'topics'),
      timestamps: merrimack.createOrOpen(db, 'timestamps'),
      topicCounters: merrimack.createOrOpen(db, 'topic-counters')
    });
  });
};
