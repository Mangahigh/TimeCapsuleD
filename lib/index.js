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

const NULL_BYTE = new Buffer([0x00]);

const getSockId = function (sock) {
  return [sock.remoteAddress, sock.remotePort].join(':');
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

  async _getRedisClientSubscriber() {
      return this._redisClientSubscriberPool.pop() || this._redisLib.connect();
  }

  async _writeToSock(sock, data) {
    await sock.write(data);
  }

  /**
   * Gets the next item from the processing queue and sends to the socket
   * @private
   */
  async _getNextAndSend(sock, data) {
    const redisClient = await this._getRedisClientSubscriber();
    let getter = new Getter(redisClient, this._keyLib, this._logger);

    try {
      getter.prepare(data.toString());

      await this._writeToSock(sock, 'OK');

      let keepAliveInterval;
      if (this._config.keepAliveInterval) {
        keepAliveInterval = setInterval(() => {
          this._writeToSock(sock, NULL_BYTE);
        }, this._config.keepAliveInterval * 1000);
      }

      const newData = await getter.get();

      if (keepAliveInterval) {
        clearTimeout(keepAliveInterval);
      }

      try {
        await this._writeToSock(sock, newData);
      } catch (e) {
        await getter.reject();
        throw e;
      }

      await getter.accept();
      return getter;

    } catch (e) {
      this._logger.error('Getting failed');
      this._logger.error(e);
      sock.end();
    }
  }

  _onConnection(sock, redisClientPublish) {
    this._logger.info('CLIENT CONNECTED: ' + getSockId(sock));

    this._writeToSock(sock, 'OK');

    let storer;
    let getter;


    sock.on('data', async (data) => {
      switch (data.toString().split(' ')[0].trim()) {
        case ACTION.STATS:
          this._logger.info('STATS CONNECTED: ' + getSockId(sock));

          const stats = await this.getStats();
          await this._writeToSock(sock, stats.join("\n") + "\n");
          sock.end();

          break;

        case ACTION.STORE:
          this._logger.info('PUBLISHER CONNECTED: ' + getSockId(sock));

          storer = new Storer(redisClientPublish, this._keyLib);
          storer.prepare(data.toString());

          this._writeToSock(sock, 'OK');

          break;

        case ACTION.FETCH:
          this._logger.info('SUBSCRIBER CONNECTED: ' + getSockId(sock));

          this._subscribers[getSockId(sock)] = sock;
          getter = await this._getNextAndSend(sock, data);

          break;

        case 'ACK':

          this._logger.info('SUBSCRIBER ACK\'ED: ' + getSockId(sock));

          if (getter) {
            await getter.ack();
            sock.end();
          } else {
            this._logger.error('Tried to ACK without a getter');
            sock.end();
          }
          break;

        default:
          if (storer) {
            this._logger.info('PUBLISHER SENT DATA: ' + getSockId(sock));
            await storer.receive(data.toString());
            await this._writeToSock(sock, 'OK');
            sock.end();
          } else {
            this._logger.error('Unknown command' + data.toString());
            sock.end();
          }
      }
    });

    sock.on('end', () => {
      return this._logger.info('Client connection ended ' + getSockId(sock));
    });

    sock.on('close', async () => {
      this._logger.info('Client connection closed ' + getSockId(sock));

      if (getter) {
        try {
          await getter.close();
        } catch (e) {
          this._logger.warn(e);
        }

        let redisClient = getter.getRedisClient();

        if (this._redisClientSubscriberPool.length < this._config.minRedisClientSubscriberPool) {
          this._redisClientSubscriberPool.push(redisClient);
        } else {
          redisClient.end(false);
        }
      }

      delete(this._subscribers[getSockId(sock)]);
    })
  }

  async getStats() {
    const redisClient = await this._redisLib.connect(true);

    const statsLib = new Stats(redisClient, this._keyLib);

    let stats = [];

    stats.push('__healthy: true');
    stats.push('__subscriberCount: ' + Object.keys(this._subscribers).length);
    stats.push('__unusedRedisConnections: ' + this._redisClientSubscriberPool.length);
    stats.push('__memory: ' + statsLib.getMemoryUsage() + ' MB');

    const queues = await this._queueLib.getAll(redisClient);

    stats.push('__queueCount: ' + queues.length);
    const data = await statsLib.getStats(queues);

    const statsString = stats.concat(data);
    redisClient.quit();

    return statsString;
  }

  /**
   * Run the server
   * @returns {TimeCapsule}
   */
  async runServer() {
    let initialConnections = [];

    initialConnections.push(this._serverLib.launch());

    // set up redis connections
    // redisClientPublish, redisClientInpender
    repeat(2, () => initialConnections.push(this._redisLib.connect(true).then((redisClient) => redisClient)));

    initialConnections.push(this._redisLib.connect(false).then((redisClient) => redisClient));

    // the connections pool for subscribers
    repeat(this._config.minRedisClientSubscriberPool, () => {
      initialConnections.push(this._redisLib.connect(true).then((redisClient) => {
        this._redisClientSubscriberPool.push(redisClient);
        return redisClient;
      }));
    });

    const [server, redisClientPublish, redisClientInpender, redisClientInpenderLock] = await Promise.all(initialConnections);

    new Inpender(this._keyLib, this._queueLib, redisClientInpender, redisClientInpenderLock, this._config).moveToPending();

    try {
      server.on('connection', (sock) => this._onConnection(sock, redisClientPublish));
    } catch (e) {
      this._logger.error(e);
    }

    return this;
  }
}

module.exports = TimeCapsule;
