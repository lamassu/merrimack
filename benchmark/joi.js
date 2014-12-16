'use strict';

var _ = require('lodash');
var Benchmark = require('benchmark');
var Joi = require('joi');

var fasterror = require('fasterror');
var ParamError = fasterror('ParamError');

var eventSchema = {
  id: Joi.string().guid(),
  topic: Joi.string().alphanum().max(10),
  mode: Joi.string().alphanum().max(10),
  host: Joi.string().hostname(),
  payload: Joi.object().optional(),
  timestamp: Joi.date().iso()
};

var requestValidator = Joi.object().keys(_.merge({}, eventSchema, {
  mode: 'request',
  requestId: Joi.string().guid(),
  source: Joi.boolean(),
  sourceId: Joi.string().guid(),
  timeout: Joi.number().integer().greater(0).max(60 * 1000),
  ttl: Joi.number().integer().greater(0).max(60 * 60 * 1000)
}));

var eventValidator = Joi.object().keys(eventSchema);

function joiErr() {
  Joi.validate({hi: 'yo'}, requestValidator, {presence: 'required'});
}

function joi() {
  return Joi.validate({
    mode: 'event',
    topic: 'test',
    payload: { msg: 'some heavy stuff' },
    id: 'f77394c4-56e1-40e5-a9f3-b9ba021f9293',
    host: 'jmbp.local',
    timestamp: '2014-12-16T05:23:47.836Z'
  }, eventValidator, {presence: 'required'});
}

var bench = new Benchmark('joiErr', joiErr);
console.log(bench.run().toString());

var bench = new Benchmark('joi', joi);
console.log(bench.run().toString());
