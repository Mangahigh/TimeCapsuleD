var keys = require('./keys');

module.exports = {
  initialise:(config) => {
    let keyLib = keys.initialise(config);

    return {
      getAll: (redisClient, successCallback) => {
        redisClient.smembers(keyLib.getName('queues'), (err, keys) => successCallback(keys))
      }
    }
  }
};
