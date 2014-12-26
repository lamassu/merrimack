'use strict';

//var P = require('bluebird');
var chai = require('chai');
var chaiAsPromised = require("chai-as-promised");

chai.use(chaiAsPromised);
var should = chai.should();
chai.config.includeStack = true;

var fdb = require('../lib/fdb');

describe('fdb', function() {
  beforeEach(function() {
    return fdb.init()
    .then(function() {
      return fdb.nuke();
    }).should.be.fulfilled;
  });

  it('should consume nothing', function() {
    return fdb.consume('something', 'groupA').should.eventually.be.null;
  });

  it('should produce and consume once', function() {
    var msg1 = new Buffer('a message');
    return fdb.produce('something', msg1)
    .then(function() {
      return fdb.consume('something', 'groupA');
    })
    .then(function(msg) {
      msg.should.eql(msg1);
      return fdb.consume('something', 'groupA').should.eventually.be.null;
    });
  });

  it('should produce and consume twice for each group', function() {
    var msg1 = new Buffer('a message');
    var msg2 = new Buffer('another message');
    return fdb.produce('something', msg1)
    .then(function() {
      return fdb.produce('something', msg2);
    })
    .then(function() {
      return fdb.consume('something', 'groupA');
    })
    .then(function(msg) {
      msg.should.eql(msg1);
      return fdb.consume('something', 'groupA');
    })
    .then(function(msg) {
      msg.should.eql(msg2);
      return fdb.consume('something', 'groupA');
    })
    .then(function(msg) {
      should.not.exist(msg);
      return fdb.consume('something', 'groupB');
    })
    .then(function(msg) {
      msg.should.eql(msg1);
      return fdb.consume('something', 'groupB');
    })
    .then(function(msg) {
      msg.should.eql(msg2);
      return fdb.consume('something', 'groupB').should.eventually.be.null;
    });
  });

  it('should produce, consume, produce, consume', function() {
    var msg1 = new Buffer('a message');
    var msg2 = new Buffer('another message');
    return fdb.produce('something', msg1)
    .then(function() {
      return fdb.consume('something', 'groupA');
    })
    .then(function(msg) {
      msg.should.eql(msg1);
      return fdb.produce('something', msg2);
    })
    .then(function() {
      return fdb.consume('something', 'groupA');
    })
    .then(function(msg) {
      msg.should.eql(msg2);
      return fdb.consume('something', 'groupA').should.eventually.be.null;
    });
  });
});
