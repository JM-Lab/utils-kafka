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
 * The type Jm output server.
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
     * Instantiates a new Jm output server.
     *
     * @param zookeeperConnect the zookeeper connect
     */
    public JMKafkaServer(String zookeeperConnect) {
        this(zookeeperConnect, DEFAULT_KAFKA_LOG);
    }

    /**
     * Instantiates a new Jm output server.
     *
     * @param zookeeperConnect the zookeeper connect
     * @param logDir           the log dir
     */
    public JMKafkaServer(String zookeeperConnect, String logDir) {
        this(zookeeperConnect, logDir, 1);
    }


    /**
     * Instantiates a new Jm output server.
     *
     * @param zookeeperConnect the zookeeper connect
     * @param serverPort       the server port
     */
    public JMKafkaServer(String zookeeperConnect, int serverPort) {
        this(zookeeperConnect, serverPort, DEFAULT_KAFKA_LOG);
    }

    /**
     * Instantiates a new Jm output server.
     *
     * @param zookeeperConnect the zookeeper connect
     * @param serverPort       the server port
     * @param logDir           the log dir
     */
    public JMKafkaServer(String zookeeperConnect, int serverPort,
            String logDir) {
        this(zookeeperConnect, serverPort, logDir, 1);
    }

    /**
     * Instantiates a new Jm output server.
     *
     * @param zookeeperConnect              the zookeeper connect
     * @param logDir                        the log dir
     * @param offsetsTopicReplicationFactor the offsets topic replication factor
     */
    public JMKafkaServer(String zookeeperConnect, String logDir,
            int offsetsTopicReplicationFactor) {
        this(zookeeperConnect, 9092, logDir, offsetsTopicReplicationFactor);
    }

    /**
     * Instantiates a new Jm output server.
     *
     * @param zookeeperConnect              the zookeeper connect
     * @param serverPort                    the server port
     * @param logDir                        the log dir
     * @param offsetsTopicReplicationFactor the offsets topic replication factor
     */
    public JMKafkaServer(String zookeeperConnect, int serverPort, String logDir,
            int offsetsTopicReplicationFactor) {
        this(zookeeperConnect, serverPort, logDir,
                offsetsTopicReplicationFactor, new Properties());
    }

    /**
     * Instantiates a new Jm output server.
     *
     * @param zookeeperConnect              the zookeeper connect
     * @param serverPort                    the server port
     * @param logDir                        the log dir
     * @param offsetsTopicReplicationFactor the offsets topic replication factor
     * @param kafkaServerProperties         the output server properties
     */
    public JMKafkaServer(String zookeeperConnect, int serverPort, String logDir,
            int offsetsTopicReplicationFactor,
            Properties kafkaServerProperties) {
        this(kafkaServerProperties);
        this.kafkaServerProperties.put("zookeeper.connect", zookeeperConnect);
        this.kafkaServerProperties.put("offsets.topic.replication.factor",
                String.valueOf(offsetsTopicReplicationFactor));
        this.kafkaServerProperties.put(LOG_DIR, logDir);
        this.kafkaServerProperties.put("port", serverPort);
        this.kafkaServerProperties
                .put("listeners", "PLAINTEXT://:" + serverPort);
    }

    /**
     * Instantiates a new Jm output server.
     *
     * @param kafkaServerProperties the output server properties
     */
    public JMKafkaServer(Properties kafkaServerProperties) {
        this.kafkaServerProperties = kafkaServerProperties;
    }


    /**
     * Start jm output server.
     *
     * @return the jm output server
     */
    public JMKafkaServer start() {
        this.kafkaServer =
                KafkaServerStartable.fromProps(kafkaServerProperties);
        this.kafkaServerConnect =
                JMString.buildIpOrHostnamePortPair(OS.getIp(), getPort());
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
            JMExceptionManager.logException(log, e, "stop",
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
     * Gets output server connect.
     *
     * @return the output server connect
     */
    public String getKafkaServerConnect() {
        return this.kafkaServerConnect;
    }

    /**
     * Gets output server properties.
     *
     * @return the output server properties
     */
    public Properties getKafkaServerProperties() {
        return this.kafkaServerProperties;
    }

    /**
     * Gets output log dir.
     *
     * @return the output log dir
     */
    public String getKafkaLogDir() {
        return this.kafkaServerProperties.getProperty(LOG_DIR);
    }

}
