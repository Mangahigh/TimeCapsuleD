const syslog = require('syslog');
const merge = require('lodash.merge');

class Logger {
  /**
   * @private
   * @param {Object} [config]
   */
  constructor(config) {
    const sysConfig = merge(
      {
        host: '127.0.0.1',
        port: 514,
        identifier: 'timecapsule'
      },
      config ? config.syslog : {}
    );

    this._config = config;

    this._sysConsole = syslog.createClient(sysConfig.port, sysConfig.host, {name: sysConfig.identifier});
  }

  /** @private */
  _log(type) {
    const messages = Array.prototype.slice.call(arguments).splice(1);

    if (this._config.log === 'console' || this._config.log === 'combined') {
      console[type.toLowerCase()].apply(this, messages);
    }

    if (this._config.log === 'syslog' || this._config.log === 'combined') {
      this._sysConsole.log.call(this._sysConsole, type + ': ' + messages.map(data => {
          return typeof data === 'string' ? data : JSON.stringify(data);
        }).join(' '));
    }
  }

  // ---

  info() {
    if (this._config.logLevel & 8) {
      this._log('Info', ...arguments)
    }
  }

  log() {
    if (this._config.logLevel & 4) {
      this._log('Log', ...arguments)
    }
  }

  warn() {
    if (this._config.logLevel & 2) {
      this._log('Warn', ...arguments)
    }
  }

  error() {
    if (this._config.logLevel & 1) {
      this._log('Error', ...arguments)
    }
  }

  trace() {
    this._log('Trace', ...arguments)

  }
}

module.exports = Logger;

