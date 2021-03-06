package kr.jm.utils.kafka;

import kafka.server.KafkaServerStartable;
import kr.jm.utils.enums.OS;
import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;
import kr.jm.utils.helper.JMString;
import kr.jm.utils.helper.JMThread;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The type Jm kafka server.
 */
public class JMKafkaServer {
    /**
     * The constant DEFAULT_KAFKA_LOG.
     */
    public static final String DEFAULT_KAFKA_LOG = "output-log";
    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(JMKafkaServer.class);
    private static final String LOG_DIR = "log.dir";

    private String kafkaServerConnect;
    private KafkaServerStartable kafkaServer;
    private Properties kafkaServerProperties;
    private ExecutorService kafkaBrokerThreadPool;

    /**
     * Instantiates a new Jm kafka server.
     *
     * @param zookeeperConnect              the zookeeper connect
     * @param serverPort                    the server port
     * @param logDir                        the log dir
     * @param offsetsTopicReplicationFactor the offsets topic replication factor
     * @param kafkaServerProperties         the kafka server properties
     */
    private JMKafkaServer(String zookeeperConnect, String serverIp,
            int serverPort, String logDir, int offsetsTopicReplicationFactor,
            Properties kafkaServerProperties) {
        this.kafkaServerConnect =
                JMString.buildIpOrHostnamePortPair(serverIp, serverPort);
        this.kafkaServerProperties = kafkaServerProperties;
        this.kafkaServerProperties.put("zookeeper.connect", zookeeperConnect);
        this.kafkaServerProperties.put("offsets.topic.replication.factor",
                String.valueOf(offsetsTopicReplicationFactor));
        this.kafkaServerProperties.put(LOG_DIR, logDir);
        this.kafkaServerProperties.put("port", serverPort);
        this.kafkaServerProperties
                .put("listeners", "PLAINTEXT://" + this.kafkaServerConnect);
    }

    /**
     * Start jm kafka server.
     *
     * @return the jm kafka server
     */
    public JMKafkaServer start() {
        this.kafkaServer =
                KafkaServerStartable.fromProps(kafkaServerProperties);
        this.kafkaBrokerThreadPool = JMThread.newSingleThreadPool();
        JMThread.runAsync(() -> {
            Thread.currentThread().setName("JMKafkaServer-" + OS.getHostname());
            JMLog.info(log, "startup");
            kafkaServer.startup();
        }, kafkaBrokerThreadPool);
        return this;
    }

    /**
     * Stop.
     */
    public void stop() {
        log.info("shutdown starting {} ms !!!", System.currentTimeMillis());
        try {
            if (kafkaBrokerThreadPool != null) {
                kafkaBrokerThreadPool.shutdown();
                kafkaBrokerThreadPool.awaitTermination(10, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            JMExceptionManager.handleException(log, e, "stop",
                    kafkaBrokerThreadPool.shutdownNow());
        } finally {
            if (kafkaServer != null)
                kafkaServer.shutdown();
        }
        log.info("shutdown completely Over {} ms !!!",
                System.currentTimeMillis());
    }

    /**
     * Gets port.
     *
     * @return the port
     */
    public int getPort() {
        return this.kafkaServer.staticServerConfig().port();
    }

    /**
     * Gets kafka server connect.
     *
     * @return the kafka server connect
     */
    public String getKafkaServerConnect() {
        return this.kafkaServerConnect;
    }

    /**
     * Gets kafka server properties.
     *
     * @return the kafka server properties
     */
    public Properties getKafkaServerProperties() {
        return this.kafkaServerProperties;
    }

    /**
     * Gets kafka log dir.
     *
     * @return the kafka log dir
     */
    public String getKafkaLogDir() {
        return this.kafkaServerProperties.getProperty(LOG_DIR);
    }

    public static class Builder {
        private String zookeeperConnect;
        private String serverIp = OS.getIp();
        private int serverPort = 9092;
        private String logDir = JMKafkaServer.DEFAULT_KAFKA_LOG;
        private int offsetsTopicReplicationFactor = 1;
        private Properties kafkaServerProperties = new Properties();

        public Builder(String zookeeperConnect) {
            this.zookeeperConnect = zookeeperConnect;
        }

        public Builder serverIp(String serverIp) {
            this.serverIp = serverIp;
            return this;
        }

        public Builder logDir(String logDir) {
            this.logDir = logDir;
            return this;
        }

        public Builder offsetsTopicReplicationFactor(
                int offsetsTopicReplicationFactor) {
            this.offsetsTopicReplicationFactor = offsetsTopicReplicationFactor;
            return this;
        }

        public Builder serverPort(int serverPort) {
            this.serverPort = serverPort;
            return this;
        }

        public Builder kafkaServerProperties(
                Properties kafkaServerProperties) {
            this.kafkaServerProperties = kafkaServerProperties;
            return this;
        }

        public JMKafkaServer build() {
            return new JMKafkaServer(zookeeperConnect, serverIp, serverPort,
                    logDir, offsetsTopicReplicationFactor,
                    kafkaServerProperties);
        }
    }
}
