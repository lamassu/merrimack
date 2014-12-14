'use strict';

var fasterror = require('fasterror');
var Bluebird = require('bluebird');
var ByteBuffer = require('bytebuffer');
var fdb = require('fdb').apiVersion(300);
var db = fdb.open();
var _ = require('lodash');

var RANDOM_BITS = 20;
var RANDOM_FACTOR = 0x1 << RANDOM_BITS;
var ZERO_64 = packNum(0);
var SECONDARY_COUNTER_BASE = 2;

var AppConflictError = fasterror('AppConflictError');

var lcounter = 0;
var dirs;

exports.init = function init() {
  return fdb.directory.createOrOpen(db, 'merrimack')
  .then(function(merrimack) {
    return Bluebird.props({
      merrimack: merrimack,
      records: merrimack.createOrOpen(db, 'records'),
      topics: merrimack.createOrOpen(db, 'topics'),
      groups: merrimack.createOrOpen(db, 'groups'),
      timestamps: merrimack.createOrOpen(db, 'timestamps'),
      gcounter: merrimack.createOrOpen(db, 'gcounter'),
      scounters: merrimack.createOrOpen(db, 'scounters')
    })
    .then(function(result) {
      dirs = result;
      exports.directories = dirs;
      return db.max(dirs.gcounter, ZERO_64);
    });
  });
};

function backoff() {
}

function shallIncrement() {
  // whether to increment or not based on backoff percentage
  return true;
}

exports.nuke = function nuke() {
  var clears = _.map(dirs, function(dir) {
    var r = dir.range();
    return db.clearRange(r.begin, r.end);
  });
  clears.concat(db.clear(dirs.gcounter));
  return Bluebird.all(clears);
};

// TODO check everything for idempotence
exports.insertRec = function insertRec(topic, message) {
  var messageStr = JSON.stringify(message);
  var now = Date.now();

  function fdbInsertRec(tr, cb) {
    var gcounter;
    tr.snapshot.get(dirs.gcounter)
    .then(function(gcounterBuf) {
      gcounter = unpackNum(gcounterBuf);
      var sIndex = gcounter % SECONDARY_COUNTER_BASE;
      var scounterKey = dirs.scounters.pack([sIndex]);
      return tr.get(scounterKey);
    })
    .then(function(scounterBuf) {
      var scounter = unpackNum(scounterBuf);
      if (scounter !== gcounter) {
        backoff();
        return cb(new AppConflictError('scounter and gcounter don\'t match'));
        // adjust backoff
        // Error out, would be nice to treat this as a conflict to get free retries
        // Otherwise, error out and retry entire tx after timeout
      }
      var rand = Math.floor(Math.random() * RANDOM_FACTOR);
      var key = dirs.records.pack([gcounter, lcounter, rand]);
      var topicKey = dirs.topics.pack([topic, gcounter, lcounter, rand]);
      var timeKey = dirs.timestamps.pack([now, gcounter, lcounter, rand]);
      lcounter++;
      tr.set(key, messageStr);
      tr.set(topicKey, messageStr);
      tr.set(timeKey, key);

      if (shallIncrement()) {
        var newGcounter = gcounter + 1;
        var newScounterIndex = newGcounter % SECONDARY_COUNTER_BASE;
        var newScounterKey = dirs.scounters.pack([newScounterIndex]);
        tr.max(dirs.gcounter, packNum(newGcounter));
        tr.set(newScounterKey, packNum(newGcounter));
      }
      return cb();
    })
    .catch(function(err) {
      //adjust backoff
      return cb(err);
    });
  }

  return db.doTransaction(fdbInsertRec);
};

exports.consume = function consume(topic, group) {
  function fdbConsume(tr, cb) {
    var scountersRange = dirs.scounters.range();
    var scounterMin;
    var iter = tr.snapshot.getRange(scountersRange.begin, scountersRange.end);
    iter.toArray()
    .then(function(arr) {
      scounterMin = _.min(arr, function(keyPair) {
        return unpackNum(keyPair.value);
      });
      return tr.get(dirs.groups.pack([group]));
    })
    .then(function(offsetKey) {
      if (!offsetKey) {
        var topicKey= dirs.topics.pack([topic]);
        var topicKeySelector = fdb.KeySelector.firstGreaterOrEqual(topicKey);
        return tr.snapshot.getKey(topicKeySelector);
      }
      var offsetKeySelector = fdb.KeySelector.firstGreaterThan(offsetKey);
      return tr.snapshot.getKey(offsetKeySelector);
    })
    .then(function(key) {
      // No records left to process
      if (!dirs.topics.contains(key)) return null;

      var counter = dirs.topics.unpack(key)[1];
      if (counter >= scounterMin) return null;
      tr.set(dirs.groups.pack([group]), key);
      return tr.snapshot.get(key);
    })
    .then(function(recBuf) {
      if (!recBuf) return cb(null, null);
      return cb(null, JSON.parse(recBuf));
    })
    .catch(function(err) {
      return cb(err);
    });
  }

  return db.doTransaction(fdbConsume);
};

function unpackNum(buf) {
  if (!buf) return 0;
  var bb = new ByteBuffer.wrap(buf, 'binary', ByteBuffer.LITTLE_ENDIAN);
  return bb.readLong(0).toNumber();
}

function packNum(num) {
  return new ByteBuffer(8, ByteBuffer.LITTLE_ENDIAN).writeUint64(num).buffer;
}
