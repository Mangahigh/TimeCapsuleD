class Stats {
  constructor(redisClientStats, keyLib) {
    this._redisClientStats = redisClientStats;
    this._keyLib = keyLib;
  }

  _getFutureCount(queueName) {
    return new Promise((resolve) => {
      this._redisClientStats.zcount(this._keyLib.getName('index', queueName), '-inf', '+inf', (err, count) => {
        resolve([queueName, 'future', count]);
      })
    });
  }

  _getWaitingCount(queueName) {
    return new Promise((resolve) => {
      this._redisClientStats.llen(this._keyLib.getName('list', queueName), (err, count) => {
        resolve([queueName, 'queued', count]);
      });
    });
  }

  _getNextUpcomingItem(queueName) {
    return new Promise((resolve) => {
      this._redisClientStats.zrangebyscore([
        this._keyLib.getName('index', queueName),
        '-inf',
        '+inf',
        'LIMIT',
        0,
        1
      ], (err, data) => {
        if (data[0]) {
          this._redisClientStats.hgetall(this._keyLib.getName('data', queueName) + ':' + data[0], (err, data) => {
            if (data) {
              resolve([queueName, data.date]);
            }
          });
        } else {
          resolve([queueName, null]);
        }
      })
    })
  }

  getStats(queues) {
    return new Promise((resolve) => {
      let promises = [];
      let totalCount = 0;
      let stats = [];

      queues.forEach((queue) => {
        promises.push(Promise.all([
          this._getFutureCount(queue),
          this._getWaitingCount(queue),
          this._getNextUpcomingItem(queue)
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

      Promise.all(promises).then(() => {
        stats.push('__totalMessageCount: ' + totalCount);
        resolve(stats);
      })
    });
  }

}

module.exports = Stats;