#!/usr/local/bin/node

const argv = require('minimist')(process.argv.slice(2));
const fs = require("fs");
const TimeCapsule = require('../lib/index.js');

let config;

if (argv.config) {
  config = JSON.parse(argv.config);
} else if (argv['config-file']) {
  config = JSON.parse(fs.readFileSync(argv['config-file']));
}

new TimeCapsule(config).run();
