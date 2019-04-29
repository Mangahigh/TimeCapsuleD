var Redis = require('./redis');
var Keys = require('./keys');
var Queues = require('./queues');
var Inpender = require('./inpender');
var Logger = require('./logger');
var Config = require('./config');
var Server = require('./server');

module.exports = {
  start: (passedConfig) => {
    const config = Config.getConfig(passedConfig);

    let initialConnections = [];
    let redisClientSubscriberPool = [];
    let subscribers = [];

    const logger = Logger.initialise(config);
    const keyLib = Keys.initialise(config);
    const queueLib = Queues.initialise(config);
    const redisLib = Redis.initialise(config);
    const serverLib = Server.initialise(config);

    // Catch all uncaughtExceptions to prevent the server stopping when a connection is unexpectedly closed
    process.on('uncaughtException', (err) => {
      logger.error(err.toString());
    });

    initialConnections.push(serverLib.launch());

    // set up redis connections
    // redisClientPublish, redisClientListPush, redisClientStats
    for (let i = 0; i < 3; ++i) {
      initialConnections.push(redisLib.connect().then((redisClient) => redisClient));
    }

    // the connections pool for subscribers
    for (let i = 0; i < config.minRedisSubscribers; ++i) {
      initialConnections.push(redisLib.connect().then((redisClient) => {
        redisClientSubscriberPool.push(redisClient);
        return redisClient;
      }));
    }

    Promise.all(initialConnections).then((initialConnections) => {
      const [server, redisClientPublish, redisClientListPush, redisClientStats] = initialConnections;

      Inpender.initialise(keyLib, queueLib, redisClientListPush, config).moveToPending();

      const getNextEvent = (queue, callback) => {
        let redisClient = redisClientSubscriberPool.pop();

        const getNext = (redisClientSubscribe) => {
          return redisClientSubscribe.blpop(keyLib.getName('list', queue), 0, (err, data) => {
            return redisClientSubscribe.hgetall(keyLib.getName('data', queue) + ':' + data[1], (err, data) => {
              redisClientSubscriberPool.push(redisClientSubscribe);

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
            redisClientSubscriberPool.push(redisClient);
            getNext(redisClient);
          })
        } else {
          getNext(redisClient);
        }
      };

      setInterval(() => {
        let redisClient;

        while (redisClient = (redisClientSubscriberPool.length > config.minRedisSubscribers && redisClientSubscriberPool.pop())) {
          redisClient.quit();
        }

      }, 5000);

      /**
       * @param {String} queueName
       * @param {Function} successCallback
       */
      const getFutureCount = (queueName, successCallback) => {
        redisClientStats.zcount(keyLib.getName('index', queueName), '-inf', '+inf', (err, count) => successCallback(queueName, 'future', count))
      };

      /**
       * @param {String} queueName
       * @param {Function} successCallback
       */
      const getWaitingCount = (queueName, successCallback) => {
        redisClientStats.llen(keyLib.getName('list', queueName), (err, count) => successCallback(queueName, 'queued', count));
      };

      const getNextItem = (queueName, successCallback) => {
        redisClientPublish.zrangebyscore([
          keyLib.getName('index', queueName),
          '-inf',
          '+inf',
          'LIMIT',
          0,
          1
        ], (err, data) => {
          if (data[0]) {
            redisClientPublish.hgetall(keyLib.getName('data', queueName) + ':' + data[0], (err, data) => {
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
          logger.info('Client connected ' + sock.remoteAddress + ':' + sock.remotePort);

          sock.write('OK');

          let state = 'CHECK';
          let date;
          let backup;
          let queue;

          sock.on('data', (data) => {
            if (data.toString().indexOf('STATS') === 0) {
              logger.info('Client requested COUNT ' + sock.remoteAddress + ':' + sock.remotePort);

              sock.write('__subscriberCount: ' + subscribers.length + "\n");
              sock.write('__unusedRedisConnections: ' + redisClientSubscriberPool.length + "\n");

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
                queueLib.getAll(redisClientStats, getStats);
              }

            } else if (data.toString().indexOf('STORE ') === 0) {
              state = 'STORE';

              const command = data.toString().split(' ');
              queue = command[1].trim();
              date = new Date(command[2].trim());

              sock.write('OK');
            } else if (data.toString().indexOf('FETCH ') === 0) {
              logger.info('SUBSCRIBER CONNECTED: ' + sock.remoteAddress + ':' + sock.remotePort);
              state = 'FETCH';
              subscribers.push(sock);

              const command = data.toString().split(' ');
              queue = command[1].trim();

              sock.write('OK');

              getNextEvent(queue, (data) => {
                backup = data;

                try {
                  sock.write(data.data);
                  redisClientPublish.del(keyLib.getName('data', queue) + ':' + data.id, (err, count) => {
                    if (count !== 1) {
                      logger.error('FAILED TO CLEAN UP!');
                    }
                  });
                  logger.info('SUBSCRIBER RECEIVED MESSAGE: ' + sock.remoteAddress + ':' + sock.remotePort);
                } catch (e) {
                  logger.error('SUBSCRIBER FAILED TO RECEIVE MESSAGE: ' + sock.remoteAddress + ':' + sock.remotePort);
                  logger.error(e);

                  redisClientPublish.rpush(keyLib.getName('list', queue), data.id);
                }

              });
            } else {
              if (state === 'STORE') {
                logger.info('PUBLISHER SENT DATA: ' + sock.remoteAddress + ':' + sock.remotePort);

                redisClientPublish.sadd(keyLib.getName('queues'), queue);

                redisClientPublish.incr(keyLib.getName('id', queue), (err, id) => {
                  redisClientPublish.hmset([keyLib.getName('data', queue) + ':' + id, 'id', id, 'date', +date, 'data', data.toString()]);
                  redisClientPublish.zadd([keyLib.getName('index', queue), +date, id]);
                });

                sock.write('OK');
                sock.end();
              } else if (state === 'FETCH' && data.toString().indexOf('ACK') === 0) {
                backup = undefined;
                logger.info('SUBSCRIBER ACKNOWLEDGED MESSAGE: ' + sock.remoteAddress + ':' + sock.remotePort);
                sock.end();
              } else {
                sock.end();
              }
            }

          });

          sock.on('end', () => logger.info('Client connection ended ' + sock.remoteAddress + ':' + sock.remotePort));

          sock.on('close', () => {
            logger.info('Client connection closed ' + sock.remoteAddress + ':' + sock.remotePort);

            if (backup) {
              logger.warn('Message returned to queue');
              redisClientPublish.rpush(keyLib.getName('list', queue), backup.id);
              redisClientPublish.hmset([keyLib.getName('queues') + ':' + backup.id, 'id', backup.id, 'date', backup.date, 'data', backup.data]);
            }

            let index = subscribers.findIndex((o) => o.remoteAddress === sock.remoteAddress && o.remotePort === sock.remotePort);

            if (index !== -1) {
              subscribers.splice(index, 1);
            }
          })
        });
      } catch (e) {
        logger.error(e);
      }
    });
  }

};
