var queues = require('./queues');var keys = require('./keys');
var redlock = require('redlock');
var _ = require('lodash');

const getNow = () => new Date();

module.exports = {
  initialise:(keyLib, queueLib, redisClientListPush, config) => {
    const redlockConfig = _.merge(
      {
        driftFactor: 0.01,
        retryCount: 1,
        retryDelay: 1,
        retryJitter: 1
      },
      config.redlock
    );

    const lockDuration = 1000;

    const redlockClient = new redlock([redisClientListPush], redlockConfig);

    redlockClient.on('clientError', (err) => console.error('A redis error has occurred:', err));

    const unlockAndQueue = (lock) => {
      lock.unlock().then(() => setTimeout(moveToPending, config.waitInterval * 1000));
    };
    
    const moveItemToPending = (item, queue, callback) => {
      redisClientListPush.multi()
        .rpush(keyLib.getName('list', queue), item)
        .zrem(keyLib.getName('index', queue), item)
        .exec(() => callback());
    };

    const moveToPending = () => {
      queueLib.getAll(redisClientListPush, (queues) => {
        if (!queues || !queues.length) {
          setTimeout(moveToPending, config.waitInterval * 1000);
        }

        redlockClient.lock(keyLib.getName('requeueLock'), lockDuration).then((lock) => {
          queues.forEach((queue) => {
            lock.extend(lockDuration).then( () => {
                redisClientListPush.zrangebyscore([keyLib.getName('index', queue), 0, getNow().getTime()], (err, data) => {
                  let remainingItems = data.length;

                  if (!remainingItems) {
                    unlockAndQueue(lock);
                  }

                  data.forEach((item) => {
                    moveItemToPending(item, queue, () => {
                      --remainingItems;

                      if (!remainingItems) {
                        unlockAndQueue(lock);
                      }
                    });
                  });
                });
              });
          });
        });
      });
    };

    return { moveToPending }
  }
};
