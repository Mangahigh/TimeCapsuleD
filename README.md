TimeCapsule
===========

A queue that doesn't release messages until a specified time.

TimeCapsule accepts a piece of data, and a date from a publisher. It will return this data to a subscriber once the date has passed.

It is designed to be used for scheduling once off events, at scale, with redundancy and consistency.

Quick Start
-----------

```npm install -g timecapsule```

```timecapsule```

We recommend using a tool like [forever](https://www.npmjs.com/package/forever) or [pm2](https://www.npmjs.com/package/pm2) to keep your process running. 

Example use cases:
------------------

- Remind a user to verify their email a week after they signed up
- Send a subscription reminder before a subscription ends
- Complete a time bound leaderboard and notify the winners at the end date

Core principles:
----------------

1. A message will only be delivered once
2. A message will never be delivered before the end date
3. In the event of a failure then messages will not be lost
4. If a message is not acknowledged by the subscriber then it will be resent

If there are not enough subscribers to handle the number of messages becoming available then messages will be sent later that expected.

If a subscriber handles a message, but fails to acknowledge then the message may be processed twice.

You cannot delete messages once they are published. It is the responsibility of the subscriber to ensure that the message is still valid.

Technology:
-----------

Currently TimeCapsule is written in Node, and uses a Redis database to store messages.

Configuration:
--------------

Time capsule runs on 127.0.0.1:1777 by default, and requires a Redis database to be available at 127.0.0.1:6379. These can be configured by either

1. Passing a json config to the command line parameter: ```--config```.
    e.g. ```timecapsule --config --config='{"redis": {"host":"localhost"}}'```
    
2. Passing the location of a json config file the command line parameter: ```--config-file```.
    e.g ```timecapsule --config-file=config.json```
    
The current config options available are:
- __host__: (default: *127.0.0.1*) The host name that timecapsule will listen on. 
- __port__: (default: *1777*) The port that timescapsule will listen on
- __log__: (default: *false*) If log messages should be written to the console
- __redis.host__: (default *127.0.0.1*) The host name for the redis database
- __redis.port__: (default *6379*) The port for the redis database
 
 Example:
 ```
 {
    "host": "127.0.0.1",
    "port": 1777,
    "log": false,
    "redis": {
        "host": "127.0.0.1",
        "port": 6379"
    }
 }
 ```

Publishers/Subscribers:
-----------------------

It is simple to write a new publisher/subscriber library. The following libraries are already available:

- PHP [TimeCapsule PHP Client]()

Influences:
-----------

- RabbitMq - https://www.rabbitmq.com
- Linux at, batch, atq, atrm - https://linux.die.net/man/1/at

Contributing:
-------------

Please get involved in making this project better! Please submit a pull request with your changes. 

Take a look at [TODO.md]() for inspiration of things you could work on.

Licenses:
---------

Released under the MIT license. See [LICENSE]() for more details.

Acknowledgments:
----------------

Mangahigh - https://www.mangahigh.com

