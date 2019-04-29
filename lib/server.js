var Logger = require('./logger');
var net = require('net');

module.exports = {
  initialise:(config) => {
    let logger = Logger.initialise(config);

    return {
      launch: () => {
        return new Promise((resolve, reject) => {
          var server = net.createServer();

          server.listen(config.port, config.host, () => {
            logger.info('TimeCapsule is running on ' + (config.host ? config.host + ':' : 'port ') + config.port + '.');
            resolve(server);
          });

          server.on('error', (e) => {
            logger.error(e);
            reject();
          });
        });
      }
    }
  }
};
