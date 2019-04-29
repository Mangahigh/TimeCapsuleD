var syslog = require('syslog');

module.exports = {
  initialise:(config) => {
    const sysConsole = syslog.createClient(514, '127.0.0.1', {name: 'timecapsule'});

    const log = (type, arguments) => {
      if (config.log === 'syslog' || config.log === 'combined') {
        sysConsole[type === 'trace' ? 'error' : type].apply(sysConsole, [].map.call(arguments, data => JSON.stringify(data)));
      }

      if (config.log === 'console' || config.log === 'combined') {
        console[type].apply(this, arguments);
      }
    };

    return {
        info: (...arguments) => log('info', arguments),
        log: (...arguments) => log('log', arguments),
        warn: (...arguments) => log('warn', arguments),
        error: (...arguments) => log('error', arguments),
        trace: (...arguments) => log('trace', arguments)
    }
  }
};
