class Getter {
  /** @private */
  constructor(redisClient, keyLib, logger) {
    this._redisClient = redisClient;
    this._keyLib = keyLib;
    this._logger = logger;

    this._backup = null;
  }

  _getNextPendingItem(redisClient, queue) {
    return new Promise((resolve, reject) => {
      redisClient.blpop(this._keyLib.getName('list', queue), 0, (err, nextItem) => {
        if (nextItem) {
          redisClient.hgetall(this._keyLib.getName('data', queue) + ':' + nextItem[1], (err, data) => {
            if (data) {
              resolve(data);
            } else {
              this._logger.error('Something went wrong with getting item:' + nextItem[1]);
              reject();
            }
          });
        } else {
          this._logger.error('Something went wrong with getting next item from queue:' + queue);
          reject();
        }
      });
    });
  }

  // ---

  prepare(message) {
    return new Promise((resolve) => {
      this._queue = message.split(' ')[1].trim();
      resolve();
    });
  }

  get() {
    return new Promise((resolve, reject) => {
      try {
        this._getNextPendingItem(this._redisClient, this._queue).then((data) => {
          this._backup = data;

          resolve(data.data);
        }).catch(() => reject());
      } catch (e) {
        reject();
      }
    });
  }

  accept() {
    return new Promise((resolve, reject) => {
      if (this._backup) {
        this._redisClient.del(this._keyLib.getName('data', this._queue) + ':' + this._backup.id, () => {
          resolve();
        });
      } else {
        reject();
      }
    });
  }

  reject() {
    if (this._backup) {
      this._redisClient.rpush(this._keyLib.getName('list', this._queue), this._backup.id);
    }

    this._backup = null;
  }

  ack() {
    return new Promise((resolve) => {
      this._backup = null;
      resolve();
    });
  }

  close() {
    return new Promise((resolve, reject) => {
      if (this._backup) {
        this._redisClient.rpush(this._keyLib.getName('list', this._queue), this._backup.id);
        this._redisClient.hmset([this._keyLib.getName('data', this._queue) + ':' + this._backup.id, 'id', this._backup.id, 'date', this._backup.date, 'data', this._backup.data]);
        reject();
      } else {
        resolve()
      }
    });
  }

  getRedisClient() {
    return this._redisClient;
  }
}

module.exports = Getter;
