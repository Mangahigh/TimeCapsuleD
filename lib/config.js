const syslog = require('syslog');
const merge = require('lodash.merge');

const defaultConfig = {
  /**
   * The port that timeCapsule will listen on
   *
   */
  port: 1777,

  /**
   * The host that timeCapsule will listen on
   * "undefined" means that timeCapsule will accept incoming connections from all hosts
   */
  host: undefined,

  /**
   * Connection options for the redis database
   */
  redis: {
    host: '127.0.0.1',
    port: 6379,
    namespace: 'timeCapsule'
  },

  /**
   * How often the server will check for ready messages in seconds.
   * A lower number will mean that messages may be delayed
   * A higher number will increase load
   */
  waitInterval: 0.5,

  /**
   * How many redis connections will be held open in a pool for subscribers to use
   * Having more connections available will mean subscribers will start up quicker
   */
  minRedisClientSubscriberPool: 10,

  /**
   * The type of logging
   * Options are:
   *  - syslog:   Log to Syslog (127.0.0.1:514)
   *  - console:  Log to console
   *  - combined: Log to both syslog and console
   *  - false:    Disable logging
   */
  log: 'syslog',

  /**
   * A flag to indicate which logs we want (uses bitwise logic)
   * e.g
   * - 1 (error)
   * - 3 (error, warn)
   * - 7 (error, warn, log)
   * - 15 (error, warn, log, info)
   */
  logLevel: 7,

  /**
   * How often to send a keep alive signal to a client to prevent the TCP connection being closed in seconds
   * Set to false to disable the keep alive
   */
  keepAliveInterval: false,
};

module.exports = {
  getConfig:(config) => merge(defaultConfig, config || {})
};
