'use strict';

var ByteBuffer = require('bytebuffer');
var fdb = require('fdb').apiVersion(300);
var db = fdb.open();

var RANDOM_BITS = 20;
var RANDOM_FACTOR = 0x1 << RANDOM_BITS;
var ZERO_64 = new ByteBuffer(8, ByteBuffer.LITTLE_ENDIAN).writeUint64(0).buffer;

var lcounter = 0;
var directories = {
  merrimack: null,
  gcounter: null,
  records: null,
  topics: null,
  timestamps: null
};

exports.init = function init() {
  var merrimack;
  return fdb.directory.createOrOpen(db, 'merrimack')
  .then(function(_merrimack) {
    merrimack = _merrimack;
    return merrimack.createOrOpen(db, 'records');
  })
  .then(function(records) {
    directories.records = records;
    return merrimack.createOrOpen(db, 'topics');
  })
  .then(function(topics) {
    directories.topics = topics;
    return merrimack.createOrOpen(db, 'gcounter');
  })
  .then(function(gcounter) {
    directories.gcounter = gcounter;
    return merrimack.createOrOpen(db, 'timestamps');
  })
  .then(function(timestamps) {
    directories.timestamps = timestamps;
    return db.max(directories.gcounter, ZERO_64);
  });
};

exports.directories = directories;

exports.insertRec = function insertRec(topic, message) {
  var messageStr = JSON.stringify(message);
  var now = Date.now();

  function fdbInsertRec(tr, cb) {
    tr.get(directories.gcounter)
    .then(function(gcounterBuf) {
      var gcounter = unpackNum(gcounterBuf);
      var rand = Math.floor(Math.random() * RANDOM_FACTOR);
      var key = directories.records.pack([gcounter, lcounter, rand]);
      var topicKey = directories.topics.pack([topic, gcounter, lcounter, rand]);
      var timeKey = directories.timestamps.pack([now, gcounter, lcounter, rand]);
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
    var r = directories.records.range();
    tr.snapshot.getKey(fdb.KeySelector.lastLessThan(r.end))
    .then(function(key) {
      var gcounter = directories.records.unpack(key)[0] + 1;
      var gcounterBuf = new ByteBuffer(8, ByteBuffer.LITTLE_ENDIAN).
        writeUint64(gcounter).buffer;
      tr.max(directories.gcounter, gcounterBuf);
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
