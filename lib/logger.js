var syslog = require('syslog');
var _ = require('lodash');

class Logger {
  /**
   * @private
   * @param {Object} [config]
   */
  constructor(config) {
    const sysConfig = _.merge(
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

    if (this._config.log === 'syslog' || this._config.log === 'combined') {
      this._sysConsole[type === 'Trace' ? 'Error' : type.toLowerCase()].call(this._sysConsole, type + ': ' + messages.map(data => {
        return typeof data === 'string' ? data : JSON.stringify(data);
      }).join(' '));
    }

    if (this._config.log === 'console' || this._config.log === 'combined') {
      console[type.toLowerCase()].apply(this, messages);
    }
  }

  // ---

  info() {
    this._log('Info', ...arguments)
  }

  log() {
    this._log('Log', ...arguments)
  }

  warn() {
    this._log('Warn', ...arguments)
  }

  error() {
    this._log('Error', ...arguments)
  }

  trace() {
    this._log('Trace', ...arguments)
  }
}

module.exports = Logger;

