'use strict';

var P = require('bluebird');

var fdb = require('fdb').apiVersion(300);
var db = fdb.open();

exports.init = function init() {
  return fdb.directory.createOrOpen(db, 'merrimack')
  .then(function(merrimack) {
    return P.props({
      merrimack: merrimack,
      records: merrimack.createOrOpen(db, 'records'),
      topics: merrimack.createOrOpen(db, 'topics'),
      groups: merrimack.createOrOpen(db, 'groups'),
      timestamps: merrimack.createOrOpen(db, 'timestamps'),
      gcounter: merrimack.createOrOpen(db, 'gcounter'),
      scounters: merrimack.createOrOpen(db, 'scounters')
    });
  });
};
