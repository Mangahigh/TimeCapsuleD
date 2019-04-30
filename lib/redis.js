var Logger = require('./logger');
var redis = require('redis');

/**
 * Manages redis connections
 */
class Redis {
  /** @private */
  constructor(config) {
    /** @private */
    this._config = config;

    /** @private */
    this._logger = new Logger(config);
  }

  // ---

  connect() {
    return new Promise((resolve, reject) => {
      var client = redis.createClient(this._config.redis);

      client.on('error', (err) => {
        this._logger.error('Redis error ' + err);
        reject();
      });

      client.on('connect', () => {
        this._logger.info('Redis connected');
      });

      client.on('ready', () => {
        this._logger.info('Redis ready');
        resolve(client);
      });
    });
  }
}

module.exports = Redis;
