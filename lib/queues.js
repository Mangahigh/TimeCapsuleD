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
  async getAll(redisClient) {
    return await redisClient.smembers(this._keyLib.getName('queues'));
  }
}

module.exports = QueueManager;
