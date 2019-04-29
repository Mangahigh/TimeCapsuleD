var net = require('net');
var redis = require('redis');
var syslog = require('syslog');
var _ = require('lodash');
var Redlock = require('redlock');

const defaultConfig = {
  port: 1777,
  host: undefined,
  redis: {
    host: '127.0.0.1',
    port: 6379,
    namespace: 'timeCapsule'
  },
  waitInterval: 0.5,
  minRedisSubscribers: 5
};

let serverStatus = 'stopped';
let databaseStatus = 'not-connected';

// Catch all uncaughtExceptions to prevent the server stopping when a connection is unexpectedly closed
process.on('uncaughtException', (err) => {
  console.error(err.toString());
});

const setupConfigDefaults = (config) => _.merge(defaultConfig, config);

const setupLogging = (config) => {
  const defaultConsole = {
    info: console.info,
    log: console.log,
    warn: console.warn,
    error: console.error,
    trace: console.trace,
  };

  const sysConsole = syslog.createClient(514, '127.0.0.1', {name: 'timecapsule'});

  const log = (type, arguments) => {
    if (config.log === 'syslog' || config.log === 'combined') {
      sysConsole[type === 'trace' ? 'error' : type].apply(sysConsole, [].map.call(arguments, data => JSON.stringify(data)));
    }

    if (config.log === 'console' || config.log === 'combined') {
      defaultConsole[type].apply(this, arguments);
    }
  };

  console.info = (...arguments) => log('info', arguments);
  console.log = (...arguments) => log('log', arguments);
  console.warn = (...arguments) => log('warn', arguments);
  console.error = (...arguments) => log('error', arguments);
  console.trace = (...arguments) => log('trace', arguments);
};

const launchServer = (config) => {
  return new Promise((resolve, reject) => {
    serverStatus = 'starting';

    var server = net.createServer();

    server.listen(config.port, config.host, () => {
      console.info('TimeCapsule is running on ' + (config.host ? config.host + ':' : 'port ') + config.port + '.');
      serverStatus = 'running';

      resolve(server);
    });

    server.on('error', (e) => {
      console.error(e);
      reject();
    });
  });
};

const connectRedis = (config) => {
  return new Promise((resolve, reject) => {
    databaseStatus = 'connecting';
    var client = redis.createClient(config.redis);

    client.on("error", (err) => {
      databaseStatus = 'error';
      console.trace('Redis error ' + err);
      reject();
    });

    client.on('connect', () => {
      console.info('Redis connected');
      databaseStatus = 'connected';
      resolve(client);
    });
  });
};

module.exports = {
  start: (config) => {

    config = setupConfigDefaults(config);

    setupLogging(config);

    Promise.all([
      launchServer(config),
      connectRedis(config),
      connectRedis(config),
      connectRedis(config)
    ]).then((values) => {
      const [server, redisClientPublish, redisClientListPush, redisClientStats, ] = values;
      let subscribers = [];
      let redisClientSubscribes = [];

      for (let i = 0; i < config.minRedisSubscribers; ++i) {
        connectRedis(config).then((redisClient) => redisClientSubscribes.push(redisClient));
      }

      const getQueues = (redisClient, successCallback) => {
        redisClient.smembers(getKeyName('queues'), (err, keys) => successCallback(keys))
      };

      const redlock = new Redlock(
        [redisClientListPush],
        _.merge(
          {
            driftFactor: 0.01,
            retryCount: 1,
            retryDelay: 1,
            retryJitter: 1
          },
          config.redlock
        )
      );

      const getKeyName = (name, queue) => {
        return queue ? [config.redis.namespaces, queue, '__' + name].join('.') : [config.redis.namespaces, '__' + name].join('.');
      };

      redlock.on('clientError', (err) => console.error('A redis error has occurred:', err));

      const reQueueAll = () => {
        getQueues(redisClientListPush, (queues) => {
          return redlock.lock(getKeyName('requeueLock'), 1000).then((lock) => {

            if (queues.length === 0) {
              lock.unlock().then(() => setTimeout(reQueueAll, config.waitInterval * 1000));
            }

            queues.forEach((queue) => {
              return lock.extend(1000).then(
                () => {
                  return redisClientListPush.zrangebyscore([
                    getKeyName('index', queue),
                    0,
                    +(new Date())
                  ], (err, data) => {
                    let remainingItems = data.length;

                    if (data.length === 0) {
                      lock.unlock().then(() => setTimeout(reQueueAll, config.waitInterval * 1000));
                    }

                    data.forEach((item) => {
                      return redisClientListPush.multi()
                        .rpush(getKeyName('list', queue), item)
                        .zrem(getKeyName('index', queue), item)
                        .exec(() => {
                          --remainingItems;

                          if (remainingItems <= 0) {
                            lock.unlock().then(() => reQueueAll())
                          }
                        });
                    });
                  });
                });
            });
          });
        });
      };

      reQueueAll();

      const getNextEvent = (queue, callback) => {
        let redisClient = redisClientSubscribes.pop();

        const getNext = (redisClientSubscribe) => {
          return redisClientSubscribe.blpop(getKeyName('list', queue), 0, (err, data) => {
            return redisClientSubscribe.hgetall(getKeyName('data', queue) + ':' + data[1], (err, data) => {
              redisClientSubscribes.push(redisClientSubscribe);

              if (data) {
                callback(data, queue);
              } else {
                return getNextEvent(queue, callback);
              }
            });
          });
        };

        if (!redisClient) {
          connectRedis(config).then((redisClient) => {
            redisClientSubscribes.push(redisClient);
            getNext(redisClient);
          })
        } else {
          getNext(redisClient);
        }
      };

      setInterval(() => {
        let redisClient;

        while (redisClient = (redisClientSubscribes.length > config.minRedisSubscribers && redisClientSubscribes.pop())) {
          redisClient.quit();
        }

      }, 5000);

      /**
       * @param {String} queueName
       * @param {Function} successCallback
       */
      const getFutureCount = (queueName, successCallback) => {
        redisClientStats.zcount(getKeyName('index', queueName), '-inf', '+inf', (err, count) => successCallback(queueName, 'future', count))
      };

      /**
       * @param {String} queueName
       * @param {Function} successCallback
       */
      const getWaitingCount = (queueName, successCallback) => {
        redisClientStats.llen(getKeyName('list', queueName), (err, count) => successCallback(queueName, 'queued', count));
      };

      const getNextItem = (queueName, successCallback) => {
        redisClientPublish.zrangebyscore([
          getKeyName('index', queueName),
          '-inf',
          '+inf',
          'LIMIT',
          0,
          1
        ], (err, data) => {
          if (data[0]) {
            redisClientPublish.hgetall(getKeyName('data', queueName) + ':' + data[0], (err, data) => {
              if (data) {
                successCallback(queueName, data.date);
              }
            });
          } else {
            successCallback(queueName, null);
          }
        })
      };

      try {
        server.on('connection', (sock) => {
          console.info('Client connected ' + sock.remoteAddress + ':' + sock.remotePort);

          sock.write('OK');

          let state = 'CHECK';
          let date;
          let backup;
          let queue;

          sock.on('data', (data) => {
            if (data.toString().indexOf('STATS') === 0) {
              console.info('Client requested COUNT ' + sock.remoteAddress + ':' + sock.remotePort);

              sock.write('__serverStatus: ' + serverStatus + "\n");
              sock.write('__databaseStatus: ' + databaseStatus + "\n");
              sock.write('__subscriberCount: ' + subscribers.length + "\n");
              sock.write('__unusedRedisConnections: ' + redisClientSubscribes.length + "\n");

              const command = data.toString().split(' ');
              queue = command[1] ? command[1].trim() : undefined;

              const getStats = (keys) => {
                let totalCount = 0;
                let remaining = keys.length * 3;

                const callback = (queue, type, singleCount) => {
                  if (queue) {
                    sock.write(queue + '|' + type + ': ' + singleCount + "\n");

                    --remaining;
                    totalCount += singleCount;
                  }

                  if (remaining <= 0) {
                    sock.write('__totalMessageCount: ' + totalCount + "\n");
                    sock.end();
                  }
                };

                if (!keys.length) {
                  sock.write('__totalMessageCount: ' + totalCount + "\n");
                  sock.end();
                }

                for (var i = 0, len = keys.length; i < len; i++) {
                  getFutureCount(keys[i], callback);
                  getWaitingCount(keys[i], callback);
                  getNextItem(keys[i], (queue, data) => {
                    --remaining;
                    if (data) {
                      sock.write(queue + '|next: ' + (new Date(+data)).toISOString() + "\n");
                    }

                    if (remaining <= 0) {
                      sock.write('__totalMessageCount: ' + totalCount + "\n");
                      sock.end();
                    }
                  });
                }
              };

              if (queue) {
                getStats([queue]);
              } else {
                getQueues(redisClientStats, getStats);
              }

            } else if (data.toString().indexOf('STORE ') === 0) {
              state = 'STORE';

              const command = data.toString().split(' ');
              queue = command[1].trim();
              date = new Date(command[2].trim());

              sock.write('OK');
            } else if (data.toString().indexOf('FETCH ') === 0) {
              console.info('SUBSCRIBER CONNECTED: ' + sock.remoteAddress + ':' + sock.remotePort);
              state = 'FETCH';
              subscribers.push(sock);

              const command = data.toString().split(' ');
              queue = command[1].trim();

              sock.write('OK');

              getNextEvent(queue, (data) => {
                backup = data;

                try {
                  sock.write(data.data);
                  redisClientPublish.del(getKeyName('data', queue) + ':' + data.id, (err, count) => {
                    if (count !== 1) {
                      console.error('FAILED TO CLEAN UP!');
                    }
                  });
                  console.info('SUBSCRIBER RECEIVED MESSAGE: ' + sock.remoteAddress + ':' + sock.remotePort);
                } catch (e) {
                  console.error('SUBSCRIBER FAILED TO RECEIVE MESSAGE: ' + sock.remoteAddress + ':' + sock.remotePort);
                  console.error(e);

                  redisClientPublish.rpush(getKeyName('list', queue), data.id);
                }

              });
            } else {
              if (state === 'STORE') {
                console.info('PUBLISHER SENT DATA: ' + sock.remoteAddress + ':' + sock.remotePort);

                redisClientPublish.sadd(getKeyName('queues'), queue);

                redisClientPublish.incr(getKeyName('id', queue), (err, id) => {
                  redisClientPublish.hmset([getKeyName('data', queue) + ':' + id, 'id', id, 'date', +date, 'data', data.toString()]);
                  redisClientPublish.zadd([getKeyName('index', queue), +date, id]);
                });

                sock.write('OK');
                sock.end();
              } else if (state === 'FETCH' && data.toString().indexOf('ACK') === 0) {
                backup = undefined;
                console.info('SUBSCRIBER ACKNOWLEDGED MESSAGE: ' + sock.remoteAddress + ':' + sock.remotePort);
                sock.end();
              } else {
                sock.end();
              }
            }

          });

          sock.on('end', () => console.info('Client connection ended ' + sock.remoteAddress + ':' + sock.remotePort));

          sock.on('close', () => {
            console.info('Client connection closed ' + sock.remoteAddress + ':' + sock.remotePort);

            if (backup) {
              console.warn('Message returned to queue');
              redisClientPublish.rpush(getKeyName('list', queue), backup.id);
              redisClientPublish.hmset([getKeyName('queues') + ':' + backup.id, 'id', backup.id, 'date', backup.date, 'data', backup.data]);
            }

            let index = subscribers.findIndex((o) => o.remoteAddress === sock.remoteAddress && o.remotePort === sock.remotePort);

            if (index !== -1) {
              subscribers.splice(index, 1);
            }
          })
        });
      } catch (e) {
        console.error(e);
      }
    });
  }

};
