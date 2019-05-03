const queues = require('./queues');
const Redlock = require('redlock-async');
const sleep = require('./sleep');
const merge = require('lodash.merge');

/**
 * Get a date object
 * @returns {Date}
 */
const getNow = () => new Date();

class Impender {
  /**
   * @param {KeyManager} keyLib
   * @param {QueueManager} queueLib
   * @param {RedisClient} redisClient
   * @param {RedisClient} redlockRedisClient
   * @param {Object} config
   */
  constructor(keyLib, queueLib, redisClient, redisClientRedlock, config) {
    const redlockConfig = merge(
      {
        driftFactor: 0.01,
        retryCount: 5,
        retryDelay: 100,
        retryJitter: 200
      },
      config.redlock
    );

    /**
     * @type {KeyManager}
     * @private
     */
    this._keyLib = keyLib;

    /**
     * @type {Object}
     * @private
     */
    this._config = config;

    /**
     * @type {QueueManager}
     * @private
     */
    this._queueLib = queueLib;

    /**
     * @type {number}
     * @private
     */
    this._lockDuration = 1000;

    /**
     * @type {Redis}
     * @private
     */
    this._redisClient = redisClient;

    /**
     * @type {Redlock}
     * @private
     */
    this._redlockClient = new Redlock([redisClientRedlock], redlockConfig);

    this._redlockClient.on('clientError', (err) => console.error('A redis error has occurred:', err));
  }

  /**
   * Gets an exclusive lock, ensures that only one inpender is running
   */
  async _getLock(queue) {
    return await this._redlockClient.lock(this._keyLib.getName(['requeueLock2', queue].join(':')), this._lockDuration);
  }

  /**
   * Gets a list of items which have reached their embargo date
   * @param {string}   queue
   */
  async _getReadyItems(queue) {
    return await this._redisClient.zrangebyscore([this._keyLib.getName('index', queue), 0, getNow().getTime()]);
  }

  /**
   * Removes an item from the delayed queue, and puts it into the pending queue
   * @param {object}   item
   * @param {string}   queue
   */
  async _moveItemToPending(item, queue) {
    await this._redisClient.multi()
      .rpush(this._keyLib.getName('list', queue), item)
      .zrem(this._keyLib.getName('index', queue), item)
      .exec();
  };

  async _moveQueueToPending(queue) {
    const lock = await this._getLock(queue);

    const data = await this._getReadyItems(queue);
    const movers = [];

    data.forEach((item) => {
      movers.push(this._moveItemToPending(item, queue))
    });

    await Promise.all(movers);
    lock.unlock();
  }

  // ---

  /**
   * Move all the items that have reached their embargo date into the pending queue
   */
  async moveToPending() {
    const queueMovers = [];

    const queues = await this._queueLib.getAll(this._redisClient);

    queues.forEach((queue) => {
      queueMovers.push(this._moveQueueToPending(queue));
    });

    //await Promise.all(queueMovers);
    await sleep(this._config.waitInterval * 1000);
    this.moveToPending();
  }
}

module.exports = Impender;
