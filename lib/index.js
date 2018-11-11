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

    const host = (config && config.host) || '127.0.0.1';

    const redisOptions = {
      host: (config && config.redis && config.redis.host) || '127.0.0.1',
      port: (config && config.redis && config.redis.port) || 6379
    };

    const redisNamespace = (config && config.redis && config.redis.namespace) || 'timeCapsule';

    const enableLogger = (config && config.log) || false;
    
    function log(message) {
      if (enableLogger) {
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

    function sleep(ms) {
      return new Promise(resolve=> {
        setTimeout(resolve, ms)
      })
    }

    async function getNextEvent(queue) {
      let backup = '';

      // @TODO: this looks buggy!
      while (!backup) {
        await client.zrangebyscore([
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
                backup = data;
              })
              .zrem([redisNamespace, queue, 'index'].join('.'), data[0])
              .del([redisNamespace, queue, 'data'].join('.') + ':' + data[0])
              .exec();
          }
        });

        if (!backup) {
          await sleep(1000);
        }
      }

      return backup;
    }

    let subscribers = [];

    server.on('connection', function (sock) {
      sock.write('OK');

      let state = 'CHECK';
      let date;
      let backup;
      let queue;

      sock.on('data', function (data) {
        if (data.toString().indexOf('STORE ') === 0) {
          state = 'STORE';

          const command = data.toString().split(' ');
          queue = command[1];
          date = new Date(command[2]);

          sock.write('OK');
        } else if (data.toString().indexOf('FETCH ') === 0) {
          log('SUBSCRIBER CONNECTED: ' + sock.remoteAddress + ':' + sock.remotePort);
          state = 'FETCH';
          subscribers.push(sock);

          const command = data.toString().split(' ');
          queue = command[1];

          sock.write('OK');

          getNextEvent(queue).then(function (data) {
            backup = data;
            log('SUBSCRIBER RECEIVED MESSAGE: ' + sock.remoteAddress + ':' + sock.remotePort);
            sock.write(data.data);
          });
        } else {
          if (state === 'STORE') {
            log('PUBLISHER SENT DATA: ' + sock.remoteAddress + ':' + sock.remotePort);

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
        // nothing to do!
      });

      sock.on('close', function () {
        if (backup) {
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
  }
};
