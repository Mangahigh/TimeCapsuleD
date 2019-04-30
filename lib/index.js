const Redis = require('./redis');
const KeyManager = require('./keys');
const Queues = require('./queues');
const Inpender = require('./inpender');
const Logger = require('./logger');
const configLib = require('./config');
const Server = require('./server');
const Stats = require('./stats');
const Storer = require('./storer');
const Getter = require('./getter');
const repeat = require('./repeat');

const ACTION = {
  INIT: 'INIT',
  STORE: 'STORE',
  FETCH: 'FETCH',
  STATS: 'STATS'
};

class TimeCapsule {
  /**
   * @private
   * @param {Object} [config]
   */
  constructor(config) {
    /**
     * @private
     * @type {Object}
     */
    this._config = configLib.getConfig(config);

    /**
     * @type {Logger}
     * @private
     */
    this._logger = new Logger(this._config);

    /**
     * @type {KeyManager}
     * @private
     */
    this._keyLib = new KeyManager(this._config);

    /**
     * @type {QueueManager}
     * @private
     */
    this._queueLib = new Queues(this._keyLib);

    /**
     * @type {Redis}
     * @private
     */
    this._redisLib = new Redis(this._config);

    /**
     * @type {Server}
     * @private
     */
    this._serverLib = new Server(this._config);

    this._subscribers = {};

    // Catch all uncaughtExceptions to prevent the server stopping when a connection is unexpectedly closed
    process.on('uncaughtException', (err) => {
      this._logger.error(err.toString());
    });

    /**
     * @type {RedisClient[]}
     * @private
     */
    this._redisClientSubscriberPool = [];
  }

  _getRedisClientSubscriber() {
    return new Promise((resolve) => {
      let redisClient = this._redisClientSubscriberPool.pop();

      if (redisClient) {
        resolve(redisClient);
      } else {
        this._redisLib.connect().then((redisClient) => {
          resolve(redisClient);
        })
      }
    })
  }

  _writeToSock(sock, data) {
    return new Promise((resolve) => {
      sock.write(data, () => resolve());
    });
  }

  _onConnection(sock, redisClientPublish) {
    this._logger.info('CLIENT CONNECTED: ' + sock.remoteAddress + ':' + sock.remotePort);

    this._writeToSock(sock, 'OK');

    let storer;
    let getter;

    sock.on('data', (data) => {
      switch (data.toString().split(' ')[0].trim()) {
        case ACTION.STATS:
          this._logger.info('STATS CONNECTED: ' + sock.remoteAddress + ':' + sock.remotePort);

          this.getStats().then(stats => {
            this._writeToSock(sock, stats.join("\n") + "\n").then(() => sock.end());
          });
          break;

        case ACTION.STORE:
          this._logger.info('PUBLISHER CONNECTED: ' + sock.remoteAddress + ':' + sock.remotePort);

          storer = new Storer(redisClientPublish, this._keyLib);
          storer.prepare(data.toString()).then(() => this._writeToSock(sock, 'OK'));

          break;

        case ACTION.FETCH:
          this._logger.info('SUBSCRIBER CONNECTED: ' + sock.remoteAddress + ':' + sock.remotePort);

          this._subscribers[sock.remoteAddress + ':' + sock.remotePort] = sock;

          this._getRedisClientSubscriber().then((redisClient) => {
            getter = new Getter(redisClient, this._keyLib, this._logger);
            getter
              .prepare(data.toString())
              .then(() => this._writeToSock(sock, 'OK'))
              .then(() => getter.get(), () => sock.end())
              .then((data) => this._writeToSock(sock, data), () => getter.reject())
              .then(() => getter.accept())
              .catch(() => {
                this._logger.error('Getting failed');
                sock.end()
              });
          });
          break;

        case 'ACK':

          this._logger.info('SUBSCRIBER ACK\'ED: ' + sock.remoteAddress + ':' + sock.remotePort);

          if (getter) {
            getter.ack().then(() => sock.end());
          } else {
            this._logger.error('Tried to ACK without a getter');
            sock.end();
          }
          break;

        default:
          if (storer) {
            this._logger.info('PUBLISHER SENT DATA: ' + sock.remoteAddress + ':' + sock.remotePort);
            storer
              .receive(data.toString())
              .then(() => this._writeToSock(sock, 'OK'))
              .then(() => sock.end);
          } else {
            this._logger.error('Unknown command' + data.toString());
            sock.end();
          }
      }
    });

    sock.on('end', () => {
      return this._logger.info('Client connection ended ' + sock.remoteAddress + ':' + sock.remotePort);
    });

    sock.on('close', () => {
      this._logger.info('Client connection closed ' + sock.remoteAddress + ':' + sock.remotePort);

      if (getter) {
        getter
          .close()
          .catch(() => this._logger.warn('Message returned to queue'))
          .finally(() => {
            let redisClient = getter.getRedisClient();

            if (this._redisClientSubscriberPool.length < this._config.minRedisClientSubscriberPool) {
              this._redisClientSubscriberPool.push(redisClient);
            } else {
              redisClient.end(false);
            }
          });
      }

      delete(this._subscribers[sock.remoteAddress + ':' + sock.remotePort]);
    })
  }

  getStats() {
    return new Promise((resolve) => {
      this._redisLib.connect().then((redisClient) => {
        const statsLib = new Stats(redisClient, this._keyLib);

        let stats = [];

        stats.push('__healthy: true');
        stats.push('__subscriberCount: ' + Object.keys(this._subscribers).length);
        stats.push('__unusedRedisConnections: ' + this._redisClientSubscriberPool.length);
        stats.push('__memory: ' + statsLib.getMemoryUsage() + ' MB');

        this._queueLib.getAll(redisClient).then((queues) => statsLib.getStats(queues)).then((data) => {
          stats = stats.concat(data);
          resolve(stats);
          redisClient.quit();
        });
      })
    });
  }

  /**
   * Run the server
   * @returns {TimeCapsule}
   */
  runServer() {
    let initialConnections = [];

    initialConnections.push(this._serverLib.launch());

    // set up redis connections
    // redisClientPublish, redisClientInpender
    repeat(2, () => initialConnections.push(this._redisLib.connect().then((redisClient) => redisClient)));

    // the connections pool for subscribers
    repeat(this._config.minRedisClientSubscriberPool, () => {
      initialConnections.push(this._redisLib.connect().then((redisClient) => {
        this._redisClientSubscriberPool.push(redisClient);
        return redisClient;
      }));
    });

    Promise.all(initialConnections).then((connections) => {
      const [server, redisClientPublish, redisClientInpender] = connections;

      new Inpender(this._keyLib, this._queueLib, redisClientInpender, this._config).moveToPending();

      try {
        server.on('connection', (sock) => this._onConnection(sock, redisClientPublish));
      } catch (e) {
        this._logger.error(e);
      }

    });

    return this;
  }
}

module.exports = TimeCapsule;
