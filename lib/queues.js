class QueueManager {
  /** @private */
  constructor(keyLib) {
    this._keyLib = keyLib;
  }

  // ---

  /**
   * Get all queues
   * @param {RedisClient} redisClient
   * @param {function}    successCallback
   */
  getAll(redisClient, successCallback) {
    return redisClient.smembers(this._keyLib.getName('queues'), (err, keys) => successCallback(keys))
  }
}

module.exports = QueueManager;
