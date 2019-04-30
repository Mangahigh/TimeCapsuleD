class QueueManager {
  /** @private */
  constructor(keyLib) {
    this._keyLib = keyLib;
  }

  // ---

  /**
   * Get all queues
   * @param {RedisClient} redisClient
   */
  getAll(redisClient) {
    return new Promise((resolve, reject) => {
      return redisClient.smembers(this._keyLib.getName('queues'), (err, keys) => resolve(keys))
    });
  }
}

module.exports = QueueManager;
