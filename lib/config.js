var syslog = require('syslog');
var _ = require('lodash');

const defaultConfig = {
  port: 1777,
  host: undefined,
  redis: {
    host: '127.0.0.1',
    port: 6379,
    namespace: 'timeCapsule'
  },
  waitInterval: 0.5,
  minRedisSubscribers: 5
};

module.exports = {
  getConfig:(config) => _.merge(defaultConfig, config)
};
