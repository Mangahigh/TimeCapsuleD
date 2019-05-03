class Getter {
  /** @private */
  constructor(redisClient, keyLib, logger) {
    this._redisClient = redisClient;
    this._keyLib = keyLib;
    this._logger = logger;

    this._backup = null;
  }

  async _getNextPendingItem(redisClient, queue) {
    const nextItem = await redisClient.blpop(this._keyLib.getName('list', queue), 0);

    if (!nextItem) {
      this._logger.error('Something went wrong with getting next item from queue:' + queue);
      throw new Error('No item available');
    }

    const data = await redisClient.hgetall(this._keyLib.getName('data', queue) + ':' + nextItem[1]);

    if (!data) {
      this._logger.error('Something went wrong with getting item:' + nextItem[1]);
      return await this._getNextPendingItem(redisClient, queue);
    }

    return data;
  }

  // ---

  prepare(message) {
    this._queue = message.split(' ')[1].trim();
  }

  async get() {
    const item = await this._getNextPendingItem(this._redisClient, this._queue);
    this._backup = item;
    return item.data;
  }

  async accept() {
    if (!this._backup) {
      throw new Error('No backup to accept');
    }

    await this._redisClient.del(this._keyLib.getName('data', this._queue) + ':' + this._backup.id);
  }

  async reject() {
    if (this._backup) {
      await this._redisClient.rpush(this._keyLib.getName('list', this._queue), this._backup.id);
    }

    this._backup = null;
  }

  async ack() {
    this._backup = null;
  }

  async close() {
    if (this._backup) {
      this._redisClient.rpush(this._keyLib.getName('list', this._queue), this._backup.id);
      await this._redisClient.hmset(
        [
          this._keyLib.getName('data', this._queue) + ':' + this._backup.id,
          'id', this._backup.id,
          'date', this._backup.date,
          'data', this._backup.data
        ]);

      throw new Error('Message returned to queue');
    }
  }

  getRedisClient() {
    return this._redisClient;
  }
}

module.exports = Getter;
