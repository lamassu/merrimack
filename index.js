'use strict';

var Bluebird = require('bluebird');
var join = Bluebird.join;
var ByteBuffer = require('bytebuffer');
var fdb = require('fdb').apiVersion(300);
var db = fdb.open();

var RANDOM_BITS = 20;
var RANDOM_FACTOR = 0x1 << RANDOM_BITS;
var ZERO_64 = new ByteBuffer(8, ByteBuffer.LITTLE_ENDIAN).writeUint64(0).buffer;

var lcounter = 0;
var dirs;

exports.init = function init() {
  return fdb.directory.createOrOpen(db, 'merrimack')
  .then(function(merrimack) {
    return Bluebird.props({
      merrimack: merrimack,
      records: merrimack.createOrOpen(db, 'records'),
      topics: merrimack.createOrOpen(db, 'topics'),
      timestamps: merrimack.createOrOpen(db, 'timestamps'),
      gcounter: merrimack.createOrOpen(db, 'gcounter')
    })
    .then(function(result) {
      dirs = result;
      exports.directories = dirs;
      return db.max(dirs.gcounter, ZERO_64);
    });
  });
};

exports.insertRec = function insertRec(topic, message) {
  var messageStr = JSON.stringify(message);
  var now = Date.now();

  function fdbInsertRec(tr, cb) {
    tr.get(dirs.gcounter)
    .then(function(gcounterBuf) {
      var gcounter = unpackNum(gcounterBuf);
      var rand = Math.floor(Math.random() * RANDOM_FACTOR);
      var key = dirs.records.pack([gcounter, lcounter, rand]);
      var topicKey = dirs.topics.pack([topic, gcounter, lcounter, rand]);
      var timeKey = dirs.timestamps.pack([now, gcounter, lcounter, rand]);
      lcounter++;
      tr.set(key, messageStr);
      tr.set(topicKey, messageStr);
      tr.set(timeKey, key);
      return cb();
    })
    .catch(function(err) {
      return cb(err);
    });
  }

  return db.doTransaction(fdbInsertRec);
};

exports.incrementGlobalCounter = function incrementGlobalCounter() {
  function fdbIncrement(tr, cb) {
    var r = dirs.records.range();
    tr.snapshot.getKey(fdb.KeySelector.lastLessThan(r.end))
    .then(function(key) {
      var gcounter = dirs.records.unpack(key)[0] + 1;
      var gcounterBuf = new ByteBuffer(8, ByteBuffer.LITTLE_ENDIAN).
        writeUint64(gcounter).buffer;
      tr.max(dirs.gcounter, gcounterBuf);
      cb();
    })
    .catch(function(err) {
      cb(err);
    });
  }
  return db.doTransaction(fdbIncrement);
};

function unpackNum(buf) {
  var bb = new ByteBuffer.wrap(buf, 'binary', ByteBuffer.LITTLE_ENDIAN);
  return bb.readLong(0).toNumber();
}
