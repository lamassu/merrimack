'use strict';

// Base layer, generates basic event, error, request, and reply records

// var P = require('bluebird');
var os = require('os');
var _ = require('lodash');
var Joi = require('joi');
var uuid = require('uuid');
var fasterror = require('fasterror');

var db = require('./fdb.js');

var ParamError = fasterror('ParamError');

var DEFAULT_TTL = 30 * 1000;
var DEFAULT_TIMEOUT = 10 * 1000;

var GENERATORS = {
  event: eventMessage,
//  requests: requestMessage,
  error: errorMessage
};

var MODES = _.keys(GENERATORS);

var eventSchema = {
  id: Joi.string().guid(),
  topic: Joi.string().regex(/^[a-zA-Z0-9\.\/_:]+/).max(20),
  mode: Joi.string().alphanum().max(10),
  host: Joi.string().hostname(),
  payload: Joi.object().optional(),
  timestamp: Joi.date().iso()
};

var eventValidator = Joi.object().keys(eventSchema);

var errorValidator = Joi.object().keys(_.merge({}, eventSchema, {
  mode: 'error',
  code: Joi.number().integer().greater(0),
  error: Joi.string().max(1024),
}));

var requestSchema = exports.requestSchema = _.merge({}, eventSchema, {
  mode: 'request',
  requestId: Joi.string().guid(),
  source: Joi.boolean(),
  sourceId: Joi.string().guid(),
  timeout: Joi.number().integer().greater(0).max(60 * 1000),
  ttl: Joi.number().integer().greater(0).max(60 * 60 * 1000)
});

var requestValidator = Joi.object().keys(requestSchema);

var modeValidator = Joi.string().valid(MODES);

var validate = function validate(obj, validator) {
  var joi = Joi.validate(obj, validator, {presence: 'required'});
  if (joi.error) {
    var err = new ParamError(joi.error);
    console.log(err.stack);
    throw err;
  }
};
exports.validate = validate;

function generateBaseMessage() {
  return {
    id: uuid.v4(),
    host: os.hostname(),
    timestamp: new Date().toISOString()
  };
}

function buildMessage(base, opts, validator) {
  var msg = _.merge({}, base, opts, generateBaseMessage());
  validate(msg, validator);
  return msg;
}

function eventMessage(opts) {
  var base = {mode: 'event'};
  return buildMessage(base, opts, eventValidator);
}

exports.requestMessage = function requestMessage(opts) {
  // TODO: Base some of this stuff on original request for retries
  var base = {
    mode: 'request',
    requestId: uuid.v4(),
    source: true,
    sourceId: uuid.v4(),
    timeout: DEFAULT_TIMEOUT,
    ttl: DEFAULT_TTL
  };
  return buildMessage(base, opts, requestValidator);
};

function errorMessage(opts) {
  var base = {mode: 'error'};
  return buildMessage(base, opts, errorValidator);
}

exports.init = function init() {
 return db.init();
};

exports.produce = function produce(msg) {
/*
  validate(opts.mode, modeValidator);
  var messageBuilder = GENERATORS[opts.mode];
  var msg = messageBuilder(opts);
*/
  return db.produce(msg);
};

exports.consume = function consume(topic, group) {
  return db.consume(topic, group);
};
