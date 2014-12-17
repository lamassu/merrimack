'use strict';

// HTTP layer, generates mode=request events for HTTP records.

var _ = require('lodash');
var Joi = require('joi');
var base = require('./base');

var optsSchema = Joi.object().keys({
  service: Joi.string().alphanum().max(10),
  resource: Joi.string().alphanum().max(10),
  url: Joi.string().max(100),
  params: Joi.object().optional(),
  method: Joi.string().valid(['GET', 'POST', 'PUT', 'DELETE']),
  clientIp: Joi.string().regex(/^[\d\.\/]+$/).max(100),
  userId: Joi.string().guid(),
  accountId: Joi.string().guid(),
  userName: Joi.string().max(100),
  accountName: Joi.string().max(100)
});

var messageSchema = Joi.object().keys({
  mode: Joi.string().valid(['httpRequest']),
  payload: optsSchema
});

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
  var topic = [opts.resource, opts.method].join('/');
  var requestMessage = base.requestMessage({topic: topic});
  var rec = _.merge({
    mode: 'httpRequest'
  }, opts);

  var httpMessage = _.merge({}, requestMessage, rec);
  console.log(require('util').inspect(httpMessage, {depth: null}));
  return base.produce(httpMessage);
};

base.init()
.then(function() {
  return exports.produce({
    service: '0conf',
    resource: 'address',
    url: 'api.raqia.is/0conf/address/1b8237382733',
    params: {},
    method: 'GET',
    clientIp: '127.0.01',
    userId: '0f8241fd-dc2b-4559-be0d-7f04ccd67d5e',
    accountId: '0cf997bd-daab-4200-b298-6d1659003e83',
    userName: 'sherman',
    accountName: 'bitbybit'
  });
})
.catch(console.log);

