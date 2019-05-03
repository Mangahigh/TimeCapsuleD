class Stats {
  constructor(redisClientStats, keyLib) {
    this._redisClientStats = redisClientStats;
    this._keyLib = keyLib;
  }

  async _getFutureCount(queueName) {
    const count = await this._redisClientStats.zcount(this._keyLib.getName('index', queueName), '-inf', '+inf');

    return [queueName, 'future', count];
  }

  async _getWaitingCount(queueName) {
    const count = await this._redisClientStats.llen(this._keyLib.getName('list', queueName));

    return [queueName, 'future', count];
  }

  async _getNextUpcomingItem(queueName) {
    const data = await
      this._redisClientStats.zrangebyscore([
        this._keyLib.getName('index', queueName),
        '-inf',
        '+inf',
        'LIMIT',
        0,
        1
      ]);

    let item = null;

    if (data[0]) {
      item = await this._redisClientStats.hgetall(this._keyLib.getName('data', queueName) + ':' + data[0]);
    }

    return [queueName, item ? item.date : null];
  }

  getMemoryUsage() {
    const used = process.memoryUsage().heapUsed / 1024 / 1024;
    return Math.round(used * 100) / 100;
  }

  async getStats(queues) {
      let promises = [];
      let totalCount = 0;
      let stats = [];

      queues.forEach((queue) => {
        promises.push(Promise.all([
          this._getFutureCount(queue),
          this._getWaitingCount(queue),
          this._getNextUpcomingItem(queue),
        ]).then((data) => {
          let [future, waiting, upcoming] = data;
          let [upcomingQueue, upcomingDate] = upcoming;

          const writeSingleLine = (queue, type, singleCount) => {
            stats.push(queue + '|' + type + ': ' + singleCount);
            return singleCount;
          };

          totalCount += writeSingleLine(...future);
          totalCount += writeSingleLine(...waiting);

          stats.push(upcomingQueue + '|next: ' + (upcomingDate ? (new Date(+upcomingDate)).toISOString() : '<none>'));
        }));
      });

      await Promise.all(promises);

      stats.push('__totalMessageCount: ' + totalCount);

      return stats;
  }

}

module.exports = Stats;
