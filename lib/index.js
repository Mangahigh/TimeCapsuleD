var net = require('net');
var redis = require("redis");

let server;
let serverStatus = 'stopped';
let databaseStatus = 'not-connected';

module.exports = {
  status: function () {
    return {
      server: serverStatus,
      database: databaseStatus
    };
  },

  start: function (config) {
    const port = (config && config.port) || 1777;

    const host = (config && config.host !== undefined) ? config.host : '127.0.0.1';

    const redisOptions = {
      host: (config && config.redis && config.redis.host) || '127.0.0.1',
      port: (config && config.redis && config.redis.port) || 6379
    };

    const redisNamespace = (config && config.redis && config.redis.namespace) || 'timeCapsule';

    const logger = (config && config.log) || false;

    const waitInterval = (config && config.waitInterval) || 0.5;

    switch (logger) {
      case 'syslog':
        var syslog = require('syslog');
        var console = syslog.createClient(514, '127.0.0.1', {name: 'timecapsule'});
        break;
      case 'console':
      case false:
        break;
      default:
        console.warn('Unsupported logger: '.logger);
    }

    function log(message) {
      if (logger) {
        console.log(message);
      }
    }


    // ---

    serverStatus = 'starting';

    server = net.createServer();

    server.listen(port, host, () => {
      log('TimeCapsule is running on ' + host + ':' + port + '.');
      serverStatus = 'running';
    });

    server.on('error', function (e) {
      // Handle your error here
      log(e);
    });

    process.on('uncaughtException', function (err) {
      log(err.toString());
    });

    // ---
    // Redis

    databaseStatus = 'connecting';
    client = redis.createClient(redisOptions);

    client.on("error", function (err) {
      databaseStatus = 'error';
      log('Redis error ' + err);
    });

    client.on('connect', function () {
      log('Redis connected');
      databaseStatus = 'connected';
    });

    // ---

    let potential = true;
    let looper;

    function getNextEvent(queue, callback) {
      log('ATTEMPT TO FIND....');
      let found = false;

      if (potential) {
        client.zrangebyscore([
          [redisNamespace, queue, 'index'].join('.'),
          0,
          +(new Date()),
          'WITHSCORES',
          'LIMIT',
          0,
          1
        ], function (err, data) {
          if (data && data[0]) {
            client.multi()
              .hgetall([redisNamespace, queue, 'data'].join('.') + ':' + data[0], function (err, data) {
                if (data) {
                  found = true;
                  callback(data, queue);
                } else {
                  getNextEvent(queue, callback);
                }
              })
              .zrem([redisNamespace, queue, 'index'].join('.'), data[0])
              .del([redisNamespace, queue, 'data'].join('.') + ':' + data[0])
              .exec();
          } else {
            potential = false;
            getNextEvent(queue, callback);
          }
        });
      } else {
        looper = setTimeout(function () {
          getNextEvent(queue, callback);
        }, waitInterval * 1000);
      }
    }

    let subscribers = [];

    /**
     * @param {String} queueName
     * @param {Function} successCallback
     */
    function getCount(queueName, successCallback) {
      client.zcount([redisNamespace, queueName, 'index'].join('.'), '-inf', '+inf', function (err, count) {
        if (err) return log(err);
        successCallback(queueName, count);
      })
    }

    /**
     * @param {Function} successCallback
     */
    function getQueues(successCallback) {
      client.SMEMBERS([redisNamespace, '__queues'].join('.'), function (err, keys) {
        if (err) return log(err);
        successCallback(keys);
      })
    }

    try {
      server.on('connection', function (sock) {
        sock.write('OK');

        let state = 'CHECK';
        let date;
        let backup;
        let queue;

        sock.on('data', function (data) {
          if (data.toString().indexOf('COUNT') === 0) {
            const command = data.toString().split(' ');
            queue = command[1] ? command[1].trim() : undefined;

            if (queue) {
              getCount(queue, function (queue, singleCount) {
                sock.write(queue + ':' + singleCount + "\n");
                sock.end();
              });
            } else {
              getQueues(function (keys) {
                let totalCount = 0;
                let remaining = keys.length;

                const callback = function (queue, singleCount) {
                  sock.write(queue + ':' + singleCount + "\n");

                  --remaining;
                  totalCount += singleCount;

                  if (!remaining) {
                    sock.write('*:' + totalCount + "\n");
                    sock.end();
                  }
                };

                if (!keys.length) {
                  callback(0);
                }

                for (var i = 0, len = keys.length; i < len; i++) {
                  getCount(keys[i], callback);
                }
              });
            }
          } else if (data.toString().indexOf('STORE ') === 0) {
            state = 'STORE';

            const command = data.toString().split(' ');
            queue = command[1].trim();
            date = new Date(command[2].trim());

            sock.write('OK');
          } else if (data.toString().indexOf('FETCH ') === 0) {
            log('SUBSCRIBER CONNECTED: ' + sock.remoteAddress + ':' + sock.remotePort);
            state = 'FETCH';
            subscribers.push(sock);

            const command = data.toString().split(' ');
            queue = command[1].trim();

            sock.write('OK');

            getNextEvent(queue, function (data, queue) {
              backup = data;
              try {
                sock.write(data.data);
                log('SUBSCRIBER RECEIVED MESSAGE: ' + sock.remoteAddress + ':' + sock.remotePort);
              } catch (e) {
                log('SUBSCRIBER FAILED TO RECIEVE MESSAGE: ' + sock.remoteAddress + ':' + sock.remotePort);

                client.hmset([[redisNamespace, queue, 'data'].join('.') + ':' + backup.id, 'id', backup.id, 'date', backup.date, 'data', backup.data]);
                client.zadd([[redisNamespace, queue, 'index'].join('.'), backup.date, backup.id]);
              }
            });
          } else {
            if (state === 'STORE') {
              log('PUBLISHER SENT DATA: ' + sock.remoteAddress + ':' + sock.remotePort);

              potential = true;

              client.sadd([redisNamespace, '__queues'].join('.'), queue);

              client.incr([redisNamespace, queue, 'id'].join('.'), function (err, id) {
                client.hmset([[redisNamespace, queue, 'data'].join('.') + ':' + id, 'id', id, 'date', +date, 'data', data.toString()]);
                client.zadd([[redisNamespace, queue, 'index'].join('.'), +date, id]);
              });

              sock.write('OK');
              sock.end();
            } else if (state === 'FETCH' && data.toString().indexOf('ACK') === 0) {
              backup = undefined;
              log('SUBSCRIBER ACKNOWLEDGED MESSAGE: ' + sock.remoteAddress + ':' + sock.remotePort);
              sock.end();
            } else {
              sock.end();
            }
          }

        });

        sock.on('end', function () {
          if (looper) {
            clearTimeout(looper);
          }
        });

        sock.on('close', function () {
          if (backup) {
            log('MESSAGE RETURNED TO QUEUE');
            client.hmset([[redisNamespace, queue, 'data'].join('.') + ':' + backup.id, 'id', backup.id, 'date', backup.date, 'data', backup.data]);
            client.zadd([[redisNamespace, queue, 'index'].join('.'), backup.date, backup.id]);
          }

          let index = subscribers.findIndex(function (o) {
            return o.remoteAddress === sock.remoteAddress && o.remotePort === sock.remotePort;
          });

          if (index !== -1) {
            subscribers.splice(index, 1);
          }
        })
      });
    } catch (e) {
      // do nothing!
      log(e);
    }
  }
};
