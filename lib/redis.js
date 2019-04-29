var Logger = require('./logger');
var redis = require('redis');

module.exports = {
  initialise:(config) => {
    let logger = Logger.initialise(config);

    return {
      connect: () => {
        return new Promise((resolve, reject) => {
          var client = redis.createClient(config.redis);

          client.on("error", (err) => {
            logger.trace('Redis error ' + err);
            reject();
          });

          client.on('connect', () => {
            logger.info('Redis connected');
            resolve(client);
          });
        });
      }
    }
  }
};
