'use strict';

var crypto = require('crypto');
var fasterror = require('fasterror');
var P = require('bluebird');
var ByteBuffer = require('bytebuffer');
var fdb = require('fdb').apiVersion(300);
var db = fdb.open();

var directories = require('./directories');

var RANDOM_BYTES = 3;
var ZERO_64 = packNum(0);
var SECONDARY_COUNTER_BASE = 2;

var AppConflictError = fasterror('AppConflictError');

var lcounter = 0;
var dirs;

exports.init = function init() {
  return directories.init()
  .then(function(res) {
    dirs = res;
    exports.directories = dirs;
    db.max(dirs.gcounter, ZERO_64);
  });
};

exports.nuke = function nuke() {
  return dirs.merrimack.remove(db);
};

// TODO check everything for idempotence
exports.produce = function produce(topicId, message) {
  var now = Date.now();
  var topicCounterKey = dirs.topicCounters.pack([topicId]);

  function fdbInsertRec(tr, cb) {
    return tr.get(topicCounterKey)
    .then(function(topicCounterBuf) {
      var topicCounter = unpackNum(topicCounterBuf) || 0;
      var key = dirs.records.pack([topicCounter]);
      var topicKey = dirs.topicDir.pack([topicId, topicCounter]);
      var timeKey = dirs.timestamps.pack([now, topicCounter]);
      tr.set(key, message);
      tr.set(topicKey, message);
      tr.set(timeKey, key);
      topicCounter++;
      tr.set(topicCounterKey, packNum(topicCounter));

      return cb();
    })
    .catch(function(err) {
      console.log(err);
      return cb(err);
    });
  }

  return db.doTransaction(fdbInsertRec);
};

// TODO: Redo this
// No need for groups at this point.
// Just take a lastCount and return all new records.
exports.consume = function consume(topic, group) {
  function fdbConsume(tr, cb) {
    var gcounter;
    var topicDir;

    return P.resolve(dirs.topics.createOrOpen(tr, [topic]))
    .then(function(_topicDir) {
      topicDir = _topicDir;
      return tr.snapshot.get(dirs.gcounter);
    })
    .then(function(_gcounter) {
      gcounter = unpackNum(_gcounter);
      return tr.get(dirs.groups.pack([group]));
    })
    .then(function(offsetKey) {
      if (!offsetKey) {
        var topicKeySelector = fdb.KeySelector.firstGreaterOrEqual(topicDir);
        return tr.snapshot.getKey(topicKeySelector);
      }
      var offsetKeySelector = fdb.KeySelector.firstGreaterThan(offsetKey);
      return tr.snapshot.getKey(offsetKeySelector);
    })
    .then(function(key) {

      // No records left to process
      if (!topicDir.contains(key)) return null;

      var counter = topicDir.unpack(key)[0];

      if (counter >= gcounter) return null;
      tr.set(dirs.groups.pack([group]), key);
      return tr.snapshot.get(key);
    }).nodeify(cb);
  }

  return db.doTransaction(fdbConsume);
};

function unpackNum(buf) {
  if (!buf) return null;
  var bb = new ByteBuffer.wrap(buf, 'binary', ByteBuffer.LITTLE_ENDIAN);
  return bb.readLong(0).toNumber();
}

function packNum(num) {
  return new ByteBuffer(8, ByteBuffer.LITTLE_ENDIAN).writeUint64(num).buffer;
}
