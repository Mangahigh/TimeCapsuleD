const express = require('express');
const prometheusClient = require('prom-client');
const prometheusRegister = prometheusClient.register;
const prometheusCollectDefaultMetrics = prometheusClient.collectDefaultMetrics;
const Logger = require('./logger');

const app = express();
app.use(express.json());

class PrometheusServer {
    /**
     * @private
     * @param {Object} [config]
     */
    constructor(config) {
        /**
         * @private
         * @type {Object}
         */
        this._config = config;

        prometheusCollectDefaultMetrics({timeout: this._config.prometheus.interval});

        /** @private */
        this._logger = new Logger(config);
    }

    /**
     * Run the server
     * @returns {PrometheusServer}
     */
    async runServer() {
        const metricsPath = '/metrics';

        app.get(metricsPath, async (req, res) => {
            res.set('Content-Type', prometheusRegister.contentType);
            res.end(prometheusRegister.metrics());
        });

        app.listen(this._config.prometheus.port, () => {
            this._logger.log(`Prometheus Server running on port ${this._config.prometheus.port}`);
        });

        return this;
    }
}

module.exports = PrometheusServer;
