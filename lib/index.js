var Redis = require('./redis');
var KeyManager = require('./keys');
var Queues = require('./queues');
var Inpender = require('./inpender');
var Logger = require('./logger');
var configLib = require('./config');
var Server = require('./server');
var Stats = require('./stats');
var Storer = require('./storer');
var Getter = require('./getter');

const STATE = {
  INIT: 'INIT',
  STORE: 'STORE',
  FETCH: 'FETCH',
  STATS: 'STATS'
};

const repeat = (times, callback) => {
  for (let i = 0; i < times; ++i) {
    callback(i);
  }
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
          this._redisClientSubscriberPool.push(redisClient);
          resolve(redisClient);
        })
      }
    })
  }

  _cleanUpUnusedRedisClients() {
    let redisClient;

    while (redisClient = (this._redisClientSubscriberPool.length > this._config.minRedisSubscribers && this._redisClientSubscriberPool.pop())) {
      redisClient.quit();
    }
  }

  _getNextPendingItem(redisClient, queue) {
    return new Promise((resolve) => {
      redisClient.blpop(this._keyLib.getName('list', queue), 0, (err, data) => {
        redisClient.hgetall(this._keyLib.getName('data', queue) + ':' + data[1], (err, data) => {
          if (data) {
            resolve(data);
          } else {
            this._logger.error('BAD DATA FOUND');
            this._getNextPendingItem(redisClient, queue).then((data) => resolve(data));
          }
        });
      });
    });
  }

  _writeToSock(sock, data) {
    return new Promise((resolve) => {
      sock.write(data, () => resolve());
    });
  }

  _onConnection(sock, redisClientPublish) {
    this._logger.info('CLIENT CONNECTED: ' + sock.remoteAddress + ':' + sock.remotePort);

    this._writeToSock(sock, 'OK');

    let state = STATE.INIT;

    let storer;
    let getter;

    sock.on('data', (data) => {
      switch (data.toString().split(' ')[0].trim()) {
        case STATE.STATS:
          state = STATE.STATS;

          this._logger.info('STATS CONNECTED: ' + sock.remoteAddress + ':' + sock.remotePort);

          this.getStats().then(stats => {
            this._writeToSock(stats.join("\n") + "\n").then(() => sock.end());
          });
          break;

        case STATE.STORE:
          state = STATE.STORE;

          this._logger.info('PUBLISHER CONNECTED: ' + sock.remoteAddress + ':' + sock.remotePort);

          storer = new Storer(redisClientPublish, this._keyLib);
          storer.prepare(data.toString()).then(() => this._writeToSock(sock, 'OK'));

          break;

        case STATE.FETCH:
          state = STATE.FETCH;

          this._logger.info('SUBSCRIBER CONNECTED: ' + sock.remoteAddress + ':' + sock.remotePort);

          this._subscribers[sock.remoteAddress + ':' + sock.remotePort] = sock;

          this._getRedisClientSubscriber().then((redisClient) => {
            getter = new Getter(redisClient, this._keyLib);
            getter
              .prepare(data.toString())
              .then(() => this._writeToSock(sock, 'OK'))
              .then(() => getter.get())
              .then((data) => this._writeToSock(sock, data)).catch(() => getter.reject())
              .then(() => getter.accept()).catch(() => this._logger.error('FAILED TO CLEAN UP!'));
          });
          break;

        case 'ACK':

          this._logger.info('SUBSCRIBER ACK\'ED: ' + sock.remoteAddress + ':' + sock.remotePort);

          if (state === STATE.FETCH) {
            getter.ack().then(() => sock.end());
          } else {
            this._logger.error('Tried to ACK when state was not FETCH');
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

    sock.on('end', () => this._logger.info('Client connection ended ' + sock.remoteAddress + ':' + sock.remotePort));

    sock.on('close', () => {
      this._logger.info('Client connection closed ' + sock.remoteAddress + ':' + sock.remotePort);

      if (getter) {
        getter
          .close()
          .catch(() => this._logger.error('Message returned to queue'));

        this._redisClientSubscriberPool.push(getter.getRedisClient());
      }

      delete(this._subscribers[sock.remoteAddress + ':' + sock.remotePort]);
    })
  }

  getStats() {
    return new Promise((resolve) => {
      this._redisLib.connect().then((redisClient) => {
        const statsLib = new Stats(redisClient, this._keyLib);

        let stats = [];

        stats.push('__subscriberCount: ' + Object.keys(this._subscribers).length);
        stats.push('__unusedRedisConnections: ' + this._redisClientSubscriberPool.length);

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
    repeat(this._config.minRedisSubscribers, () => {
      initialConnections.push(this._redisLib.connect().then((redisClient) => {
        this._redisClientSubscriberPool.push(redisClient);
        return redisClient;
      }));
    });

    Promise.all(initialConnections).then((connections) => {
      const [server, redisClientPublish, redisClientInpender] = connections;

      new Inpender(this._keyLib, this._queueLib, redisClientInpender, this._config).moveToPending();

      setInterval(() => this._cleanUpUnusedRedisClients, 5 * 1000);

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
