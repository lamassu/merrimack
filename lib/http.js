'use strict';

// HTTP layer, generates mode=request events for HTTP records.

var _ = require('lodash');
var Joi = require('joi');
var base = require('./base');

var messageSchema = Joi.object().keys({
  mode: Joi.string().valid(['httpRequest']),
  payload: optsSchema
});

var optsSchema = Joi.object().keys({
  service: Joi.string().alphanum().max(10),
  action: Joi.string().alphanum().max(10),
  url: Joi.string().max(100),
  params: Joi.object().optional(),
  method: Joi.string().valid(['GET', 'POST', 'PUT', 'DELETE']),
  clientIp: Joi.string().regex(/^[\d\.\/]+$/).max(100),
  userId: Joi.string().guid(),
  accountId: Joi.string().guid(),
  userName: Joi.string().max(100),
  AccountName: Joi.string().max(100)
});

// Do baseSchema.concat(messageSchema) or something
function generator() {
  return {mode: 'httpRequest'};
}

/*
base.register('httpRequest', {
  parent: 'request',
  generator: generator,
  inputOptsSchema: optsSchema,
  outputOptsSchema: messageSchema
});
*/

// TODO: Quick and dirty bikeshedding, need to clean this all up
exports.produce = function produce(opts) {
  base.validate(opts, optsSchema);
  var requestMessage = base.requestMessage({});
  var httpMessage = _.merge({}, requestMessage, {mode: 'httpRequest'}, opts);
  return base.produce(httpMessage);
};
