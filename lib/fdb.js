'use strict';

var P = require('bluebird');
var ByteBuffer = require('bytebuffer');
var fdb = require('fdb').apiVersion(300);
var _ = require('lodash');

var db = fdb.open();
var directories = require('./directories');

var d;

exports.init = function init() {
  return directories.init()
  .then(function(res) {
    d = res;
  });
};

exports.nuke = function nuke() {
  return d.merrimack.remove(db);
};

// TODO check everything for idempotence
// TopicCounter is theoretically succeptible to contention if we're getting

/*  > 50 events/s on a givent topic. We could fix this by:
      - backing off on updating the counter in case of conflict
      - adding a random nonce to the key to avoid overwriting records
      - checking that our chosen key, including nonce, does not exit
        (this won't conflict because our random nonce probably wasn't chosen)
      - only returning events with a counter of one less than the current topicCounter
      - periodically (100ms) incrementing the counter if necessary
        (in case traffic has suddenly stopped, so consumers don't have to wait
        for next event to get all pending events on current topicCounter)
  However, we don't expect anywhere near this volume for our current needs,
  so this would be premature optimization. Events should be rate limited, probably
  on a per-user level.
*/
exports.produce = function produce(topicId, message) {
  var now = Date.now();
  var topicCounterKey = d.topicCounters.pack([topicId]);

  function fdbInsertRec(tr) {
    return tr.get(topicCounterKey)
    .then(function(topicCounterBuf) {
      var topicCounter = unpackCounter(topicCounterBuf);
      var topicKey = d.topics.pack([topicId, topicCounter]);
      var timeKey = d.timestamps.pack([now, topicCounter]);
      tr.set(topicKey, message);
      tr.set(timeKey, topicKey);
      topicCounter++;
      tr.set(topicCounterKey, packCounter(topicCounter));
    });
  }

  return doTransaction(fdbInsertRec);
};

exports.consume = function consume(topicId, lastCounter, limit) {
/*
  var b;
  if (lastCounter) {
    var lastKey = d.topics.pack([topicId, lastCounter]);
    b = fdb.KeySelector.firstGreaterThan(lastKey);
  } else {
    b = d.topics.pack([topicId]);
  }
*/
  var realLastCounter = _.isNumber(lastCounter) ? lastCounter : null;
  var lastKey = d.topics.pack([topicId, realLastCounter]);
  var b = fdb.KeySelector.firstGreaterThan(lastKey);

  var e = d.topics.range([topicId]).end;

  return db.getRange(b, e, {limit: limit})
  .then(function(arr) {
    return _.map(arr, function(pair) {
      var counter = d.topics.unpack(pair.key)[1];
      var value = pair.value;
      return {counter: counter, value: value};
    });
  });
};

function unpackCounter(buf) {
  if (!buf) return 0;
  var bb = new ByteBuffer.wrap(buf, 'binary', ByteBuffer.LITTLE_ENDIAN);
  return bb.readLong(0).toNumber();
}

function packCounter(num) {
  return new ByteBuffer(8, ByteBuffer.LITTLE_ENDIAN).writeUint64(num).buffer;
}

function doTransaction(cb) {
  return db.doTransaction(function(tr, lcb) {
    var promise = cb(tr);
    return P.resolve(promise).nodeify(lcb);
  });
}
