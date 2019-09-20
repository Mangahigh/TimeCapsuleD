class Storer {
  /** @private */
  constructor(redisClient, keyLib) {
    this._redisClient = redisClient;
    this._keyLib = keyLib;
  }

  // ---

  prepare(message) {
    const command = message.split(' ');

    if (command.length !== 3) {
      throw new Error('Invalid command string. Must provide "STORE <Queue Name> <Embargo Date>"');
    }

    this._queue = command[1].trim();
    this._date = new Date(command[2].trim());

    if (
        !this._date instanceof Date ||
        !this._date.getTime() ||
        this._date.getTime() < 0
    ) {
      throw new Error('Invalid date "' + command[2].trim() + '". Must be a RFC 2822 formatted date');
    }
  }

  async receive(data) {
    this._redisClient.sadd(this._keyLib.getName('queues'), this._queue);

    const id = await this._redisClient.incr(this._keyLib.getName('id', this._queue));

    await this._redisClient.hmset([this._keyLib.getName('data', this._queue) + ':' + id, 'id', id, 'date', this._date.getTime(), 'data', data.toString()]);
    await this._redisClient.zadd([this._keyLib.getName('index', this._queue), this._date.getTime(), id]);
  }
}

module.exports = Storer;
