class Storer {
  /** @private */
  constructor(redisClient, keyLib) {
    this._redisClient = redisClient;
    this._keyLib = keyLib;
  }

  // ---

  prepare(message) {
    return new Promise((resolve) => {
      const command = message.split(' ');
      this._queue = command[1].trim();
      this._date = new Date(command[2].trim());

      resolve();
    });
  }

  receive(data) {
    return new Promise((resolve) => {
      this._redisClient.sadd(this._keyLib.getName('queues'), this._queue);

      this._redisClient.incr(this._keyLib.getName('id', this._queue), (err, id) => {
        this._redisClient.hmset([this._keyLib.getName('data', this._queue) + ':' + id, 'id', id, 'date', this._date.getTime(), 'data', data.toString()]);
        this._redisClient.zadd([this._keyLib.getName('index', this._queue), this._date.getTime(), id]);

        resolve();
      });
    });
  }
}

module.exports = Storer;
