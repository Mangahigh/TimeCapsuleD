const Logger = require('./logger');
const net = require('net');

class Server {
  /** @private */
  constructor(config) {
    /** @private */
    this._config = config;

    /** @private */
    this._logger = new Logger(config);
  }

  // ---

  launch() {
    return new Promise((resolve, reject) => {
      const server = net.createServer();

      server.listen(this._config.port, this._config.host, () => {
        this._logger.log('TimeCapsule is running on ' + (this._config.host ? this._config.host + ':' : 'port ') + this._config.port + '.');
        resolve(server);
      });

      server.on('error', (e) => {
        this._logger.error(e);
        reject();
      });
    });
  }
}

module.exports = Server;
