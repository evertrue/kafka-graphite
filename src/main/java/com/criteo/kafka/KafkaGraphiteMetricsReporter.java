/*
 *  Copyright 2014 Damien Claveau
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */


package com.criteo.kafka;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.Clock;
import org.apache.log4j.Logger;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.reporting.GraphiteReporter;

import kafka.metrics.KafkaMetricsConfig;
import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;

public class KafkaGraphiteMetricsReporter implements KafkaMetricsReporter,
	KafkaGraphiteMetricsReporterMBean {

	public static final String GRAPHITE_ENABLED_CONFIG = "kafka.graphite.metrics.reporter.enabled";
	public static final String GRAPHITE_HOST_CONFIG = "kafka.graphite.metrics.host";
	public static final String GRAPHITE_PORT_CONFIG = "kafka.graphite.metrics.port";
	public static final String GRAPHITE_GROUP_CONFIG = "kafka.graphite.metrics.group";
	public static final String GRAPHITE_METRICS_EXCLUDE_REGEX_CONFIG = "kafka.graphite.metrics.exclude.regex";

	static Logger LOG = Logger.getLogger(KafkaGraphiteMetricsReporter.class);
	static String GRAPHITE_DEFAULT_HOST = "localhost";
	static int GRAPHITE_DEFAULT_PORT = 2003;
	static String GRAPHITE_DEFAULT_PREFIX = "kafka";
	
	boolean initialized = false;
	boolean running = false;
	GraphiteReporter reporter = null;
    String graphiteHost = GRAPHITE_DEFAULT_HOST;
    int graphitePort = GRAPHITE_DEFAULT_PORT;
    String graphiteGroupPrefix = GRAPHITE_DEFAULT_PREFIX;
    MetricPredicate predicate = MetricPredicate.ALL;

	@Override
	public String getMBeanName() {
		return "kafka:type=com.criteo.kafka.KafkaGraphiteMetricsReporter";
	}

	@Override
	public synchronized void startReporter(long pollingPeriodSecs) {
		if (initialized && !running) {
			reporter.start(pollingPeriodSecs, TimeUnit.SECONDS);
			running = true;
			LOG.info(String.format("Started Kafka Graphite metrics reporter with polling period %d seconds", pollingPeriodSecs));
		}
	}

	@Override
	public synchronized void stopReporter() {
		if (initialized && running) {
			reporter.shutdown();
			running = false;
			LOG.info("Stopped Kafka Graphite metrics reporter");
            try {
            	reporter = new GraphiteReporter(
						Metrics.defaultRegistry(),
						graphiteGroupPrefix,
						predicate,
						new GraphiteReporter.DefaultSocketProvider(graphiteHost, graphitePort),
						Clock.defaultClock());
            } catch (IOException e) {
            	LOG.error("Unable to initialize GraphiteReporter", e);
            }
		}
	}

	@Override
	public synchronized void init(VerifiableProperties props) {
		if (!initialized) {
			KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(props);
            graphiteHost = props.getString(GRAPHITE_HOST_CONFIG, GRAPHITE_DEFAULT_HOST);
            graphitePort = props.getInt(GRAPHITE_PORT_CONFIG, GRAPHITE_DEFAULT_PORT);
            graphiteGroupPrefix = props.getString(GRAPHITE_GROUP_CONFIG, GRAPHITE_DEFAULT_PREFIX);
            String regex = props.getString(GRAPHITE_METRICS_EXCLUDE_REGEX_CONFIG, null);

            LOG.debug("Initialize GraphiteReporter ["+graphiteHost+","+graphitePort+","+graphiteGroupPrefix+"]");

            if (regex != null) {
            	predicate = new RegexMetricPredicate(regex);
            }
            try {
            	reporter = new GraphiteReporter(
            			Metrics.defaultRegistry(),
						graphiteGroupPrefix,
						predicate,
						new GraphiteReporter.DefaultSocketProvider(graphiteHost, graphitePort),
						Clock.defaultClock());
            } catch (IOException e) {
            	LOG.error("Unable to initialize GraphiteReporter", e);
            }
            if (props.getBoolean(GRAPHITE_ENABLED_CONFIG, false)) {
            	initialized = true;
            	startReporter(metricsConfig.pollingIntervalSecs());
                LOG.debug("GraphiteReporter started.");
            }
        }
	}
}
