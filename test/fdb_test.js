'use strict';

var _ = require('lodash');
var P = require('bluebird');
var chai = require("chai");

var should = require('chai').should();
var chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
var dirtyChai = require('dirty-chai');
chai.use(dirtyChai);

var fdb = require('../lib/fdb');

describe('fdb', function() {
  beforeEach(function() {
    return fdb.init()
    .then(function() {
      return fdb.nuke();
    });
  });

  it('should consume nothing', function() {
    return fdb.consume('something').should.eventually.be.empty();
  });

  it('should produce and consume once', function() {
    var consumer = new Consumer();
    var msg1 = new Buffer('a message');
    return fdb.produce('something', msg1)
    .then(function() {
      return consumer.consume('something');
    })
    .then(function(msg) {
      msg.should.eql(msg1);
      return consumer.consume('something');
    }).should.eventually.be.empty();
  });

  it('should produce and consume twice', function() {
    var consumer = new Consumer();
    var msg1 = new Buffer('a message');
    var msg2 = new Buffer('another message');
    return fdb.produce('something', msg1)
    .then(function() {
      return fdb.produce('something', msg2);
    })
    .then(function() {
      return consumer.consume('something');
    })
    .then(function(msg) {
      msg.should.eql(msg1);
      return consumer.consume('something');
    })
    .then(function(msg) {
      msg.should.eql(msg2);
      return consumer.consume('something').should.eventually.be.null();
    });
  });

  it('should produce, consume, produce, consume', function() {
    var consumer = new Consumer();
    var msg1 = new Buffer('a message');
    var msg2 = new Buffer('another message');
    return fdb.produce('something', msg1)
    .then(function() {
      return consumer.consume('something');
    })
    .then(function(msg) {
      msg.should.eql(msg1);
      return fdb.produce('something', msg2);
    })
    .then(function() {
      return consumer.consume('something');
    })
    .then(function(msg) {
      msg.should.eql(msg2);
      return consumer.consume('something').should.eventually.be.null();
    });
  });

  it('should produce and consume a lot', function() {
    var consumer = new Consumer();

    return P.all(_.times(10, function(i) {
      var msg = new Buffer('Hi: ' + i);
      return fdb.produce('something', msg);
    })).then(function() {
      return P.reduce(_.range(12), function(total) {
        return consumer.consume('something')
        .then(function(msg) {
          return total + (msg ? 1 : 0);
        });
      }, 0);
    }).should.eventually.equal(10);
  });

});

var Consumer = function() {
  this.lastCounters = {};
};

Consumer.prototype.consume = function(topicId) {
  var self = this;
  return fdb.consume(topicId, this.lastCounters[topicId], 1)
  .then(function(arr) {
    var last = _.last(arr);
    if (last) self.lastCounters[topicId] = last.counter;
    return _.first(_.pluck(arr, 'value'));
  });
};
