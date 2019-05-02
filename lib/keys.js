class KeyManager {
  /** @private */
  constructor(config) {
    this._config = config;
  }

  // ---

  /**
   * Gets a redis key name
   * @param {string} name
   * @param {string} [queue]
   */
  getName(name, queue) {
    return queue ? [this._config.redis.namespaces, queue, '__' + name].join('.') : [this._config.redis.namespaces, '__' + name].join('.');
  }
}

module.exports = KeyManager;
