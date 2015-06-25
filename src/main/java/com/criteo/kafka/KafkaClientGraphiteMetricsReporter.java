package com.criteo.kafka;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;

/**
 * Created by tomnorden on 6/25/15.
 */
public class KafkaClientGraphiteMetricsReporter implements MetricsReporter {
    public static final String GRAPHITE_ENABLED_CONFIG = "kafka.graphite.metrics.reporter.enabled";
    public static final String GRAPHITE_HOST_CONFIG = "kafka.graphite.metrics.host";
    public static final String GRAPHITE_PORT_CONFIG = "kafka.graphite.metrics.port";
    public static final String GRAPHITE_GROUP_CONFIG = "kafka.graphite.metrics.group";
    public static final String GRAPHITE_METRICS_EXCLUDE_REGEX_CONFIG = "kafka.graphite.metrics.exclude.regex";

    private static final Logger LOG = Logger.getLogger(KafkaClientGraphiteMetricsReporter.class);
    boolean GRAPHITE_ENABLED_DEFAULT = false;
    String GRAPHITE_HOST_DEFAULT = "localhost";
    int GRAPHITE_PORT_DEFAULT = 2003;
    String GRAPHITE_PREFIX_DEFAULT = "kafka";
    String GRAPHITE_EXCLUDE_REGEX_DEFAULT = "$^";

    boolean graphiteEnabled = GRAPHITE_ENABLED_DEFAULT;
    String graphiteHost = GRAPHITE_HOST_DEFAULT;
    int graphitePort = GRAPHITE_PORT_DEFAULT;
    String graphiteGroupPrefix = GRAPHITE_PREFIX_DEFAULT;
    String excludeRegex = GRAPHITE_EXCLUDE_REGEX_DEFAULT;
    private Socket graphiteSocket;
    private PrintWriter graphiteWriter;

    @Override
    public void init(List<KafkaMetric> metrics) {
        if (graphiteEnabled) {
            try {
                initWriter();
            } catch (IOException e) {
                LOG.error("unable create initial connection to graphite, disabling reporting", e);
                graphiteEnabled = false;
            }
        }
    }

    private void initWriter() throws IOException {
        try {
            graphiteSocket = new Socket();
            graphiteSocket.connect(new InetSocketAddress(graphiteHost, graphitePort), 1000);
            graphiteWriter = new PrintWriter(graphiteSocket.getOutputStream(), true);
        } catch (IOException e) {
            LOG.error("unable to connect to graphite", e);
            close();
            throw e;
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        if (graphiteEnabled) {
            if (!metric.metricName().name().matches(excludeRegex))
            try {
                if (graphiteWriter == null) initWriter();
                graphiteWriter.printf("%s.%s.%s %f %d%n", graphiteGroupPrefix, metric.metricName().group(), metric.metricName().name(), metric.value(), System.currentTimeMillis() / 1000);
                graphiteWriter.flush();
            } catch (Exception e) {
                LOG.error(String.format("failed to send %s metric to kafka", metric.metricName().name()), e);
                close();
            }
        }

    }

    @Override
    public void close() {
        try {
            graphiteWriter.close();
            graphiteSocket.close();
        } catch (IOException e) {
            LOG.error("failed to shutdown graphite reporter", e);
        } finally {
            graphiteWriter = null;
            graphiteSocket = null;
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        graphiteEnabled = configs.containsKey(GRAPHITE_ENABLED_CONFIG) ? (Boolean) configs.get(GRAPHITE_ENABLED_CONFIG) : GRAPHITE_ENABLED_DEFAULT;
        graphiteHost = configs.containsKey(GRAPHITE_HOST_CONFIG) ? (String) configs.get(GRAPHITE_HOST_CONFIG) : GRAPHITE_HOST_DEFAULT;
        graphitePort = configs.containsKey(GRAPHITE_PORT_CONFIG) ? (Integer) configs.get(GRAPHITE_PORT_CONFIG) : GRAPHITE_PORT_DEFAULT;
        graphiteGroupPrefix = configs.containsKey(GRAPHITE_GROUP_CONFIG) ? (String) configs.get(GRAPHITE_GROUP_CONFIG) : GRAPHITE_PREFIX_DEFAULT;
        excludeRegex = configs.containsKey(GRAPHITE_METRICS_EXCLUDE_REGEX_CONFIG) ? (String) configs.get(GRAPHITE_METRICS_EXCLUDE_REGEX_CONFIG) : GRAPHITE_EXCLUDE_REGEX_DEFAULT;
    }
}
