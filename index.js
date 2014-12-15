'use strict';

// var P = require('bluebird');
var _ = require('lodash');
var Joi = require('joi');
var uuid = require('node-uuid');
var fasterror = require('fasterror');

var db = require('./lib/fdb.js');

var ParamError = fasterror('ParamError');

var DEFAULT_TTL = 30 * 1000;
var DEFAULT_TIMEOUT = 10 * 1000;

var eventSchema = {
  id: Joi.string.guid(),
  topic: Joi.string().alphanum().max(10),
  mode: Joi.string().alphanum().max(10),
  host: Joi.string().hostname(),
  payload: Joi.object().optional(),
  timestamp: Joi.date().iso()
};

var errorValidator = Joi.object().keys(_.merge({}, eventSchema, {
  mode: 'error',
  code: Joi.number().integer().greater(0),
  error: Joi.string().max(1024),
}));

var requestValidator = Joi.object().keys(_.merge({}, eventSchema, {
  mode: 'request',
  requestId: Joi.string().guid(),
  source: Joi.boolean(),
  sourceId: Joi.string().guid(),
  timeout: Joi.number().integer().greater(0).max(60 * 1000),
  ttl: Joi.number().integer().greater(0).max(60 * 60 * 1000)
}));

function validate(obj, validator) {
  var joi = Joi.validate(validator, {presence: 'required'});
  if (joi.error) throw new ParamError(joi.error);
}

function generateBaseMessage() {
  return {
    id: uuid.v4(),
    host: os.hostname(),
    timestamp: new Date().toISOString()
  };
}

function buildMessage(base, opts, validator) {
  var msg = _.merge(base, opts, generateBaseMessage());
  validate(msg, requestValidator);
  return msg;
}

function requestMessage(opts) {
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
}

function errorMessage(opts) {
  var base = {
    mode: 'error'
  };
  return buildMessage(base, opts, errorValidator);
}

