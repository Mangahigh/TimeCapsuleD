class Getter {
  /** @private */
  constructor(redisClient, keyLib) {
    this._redisClient = redisClient;
    this._keyLib = keyLib;

    this._backup = null;
  }

  _getNextPendingItem(redisClient, queue) {
    return new Promise((resolve) => {
      redisClient.blpop(this._keyLib.getName('list', queue), 0, (err, data) => {
        redisClient.hgetall(this._keyLib.getName('data', queue) + ':' + data[1], (err, data) => {
          if (data) {
            resolve(data);
          } else {
            // bad data found - this shouldn't happen
            // let's loop
            this._getNextPendingItem(redisClient, queue).then((data) => resolve(data));
          }
        });
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
        });
      } catch (e) {
        reject();
      }
    });
  }

  accept() {
    return new Promise((resolve, reject) => {
      this._redisClient.del(this._keyLib.getName('data', this._queue) + ':' + this._backup.id, (err, count) => {
        resolve();
      });
    });
  }

  reject() {
    this._redisClient.rpush(this._keyLib.getName('list', queue), this._backup.id);
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
