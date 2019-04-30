var Redis = require('./redis');
var KeyManager = require('./keys');
var Queues = require('./queues');
var Inpender = require('./inpender');
var Logger = require('./logger');
var configLib = require('./config');
var Server = require('./server');
var Stats = require('./stats');
var Storer = require('./storer');

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

    this._subscribers = [];

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

  _onConnection(sock, redisClientPublish) {
    this._logger.info('Client connected ' + sock.remoteAddress + ':' + sock.remotePort);

    sock.write('OK');

    let state = STATE.INIT;
    let date;
    let backup;
    let queue;
    let redisClientSubscribe;

    let storer;

    sock.on('data', (data) => {
      switch (data.toString().split(' ')[0].trim()) {
        case STATE.STATS:
          this._logger.info('Client requested STATS ' + sock.remoteAddress + ':' + sock.remotePort);

          this.getStats().then(stats => {
            sock.write(stats.join("\n") + "\n");
            sock.end();
          });
          break;

        case STATE.STORE:
          state = STATE.STORE;

          storer = new Storer(sock, redisClientPublish, this._keyLib, this._logger);
          storer.prepare(data.toString()).then(() => sock.write('OK'));

          break;

        case STATE.FETCH:
          this._logger.info('SUBSCRIBER CONNECTED: ' + sock.remoteAddress + ':' + sock.remotePort);
          state = STATE.FETCH;
          this._subscribers.push(sock);

          this._getRedisClientSubscriber().then((redisClient) => {
            const command = data.toString().split(' ');
            queue = command[1].trim();

            sock.write('OK');

            redisClientSubscribe = redisClient;

            this._getNextPendingItem(redisClient, queue).then((data) => {
              backup = data;

              try {
                sock.write(data.data);
                redisClient.del(this._keyLib.getName('data', queue) + ':' + data.id, (err, count) => {
                  if (count !== 1) {
                    this._logger.error('FAILED TO CLEAN UP!');
                  }
                });
                this._logger.info('SUBSCRIBER RECEIVED MESSAGE: ' + sock.remoteAddress + ':' + sock.remotePort);
              } catch (e) {
                this._logger.error('SUBSCRIBER FAILED TO RECEIVE MESSAGE: ' + sock.remoteAddress + ':' + sock.remotePort);
                this._logger.error(e);

                redisClient.rpush(keyLib.getName('list', queue), data.id);
              }
            }).catch(() => {
              this._logger.error('BAD DATA FOUND, ABORTING');
              sock.end();
            });

          });
          break;

        case 'ACK':
          if (state === STATE.FETCH) {
            backup = undefined;
            this._logger.info('SUBSCRIBER ACKNOWLEDGED MESSAGE: ' + sock.remoteAddress + ':' + sock.remotePort);
            sock.end();
          } else {
            this._logger.error('Tried to ACK when state was not FETCH');
            sock.end();
          }
          break;

        default:
          if (state === STATE.STORE) {
            this._logger.info('PUBLISHER SENT DATA: ' + sock.remoteAddress + ':' + sock.remotePort);
            storer.receive(data.toString()).then(() => {
              sock.write('OK');
              sock.end();
            });
          } else {
            this._logger.error('Unknown command' + data.toString());
            sock.end();
          }

      }

    });

    sock.on('end', () => this._logger.info('Client connection ended ' + sock.remoteAddress + ':' + sock.remotePort));

    sock.on('close', () => {
      this._logger.info('Client connection closed ' + sock.remoteAddress + ':' + sock.remotePort);

      if (redisClientSubscribe) {
        if (backup) {
          this._logger.warn('Message returned to queue');
          redisClientSubscribe.rpush(this._keyLib.getName('list', queue), backup.id);
          redisClientSubscribe.hmset([this._keyLib.getName('queues') + ':' + backup.id, 'id', backup.id, 'date', backup.date, 'data', backup.data]);
        }

        this._redisClientSubscriberPool.push(redisClientSubscribe);
      }

      let index = this._subscribers.findIndex((o) => o.remoteAddress === sock.remoteAddress && o.remotePort === sock.remotePort);

      if (index !== -1) {
        this._subscribers.splice(index, 1);
      }
    })
  }

  getStats() {
    return new Promise((resolve) => {
      this._redisLib.connect().then((redisClient) => {
        const statsLib = new Stats(redisClient, this._keyLib);

        let stats = [];

        stats.push('__subscriberCount: ' + this._subscribers.length);
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
    // redisClientPublish, redisClientInpender, redisClientStats
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
