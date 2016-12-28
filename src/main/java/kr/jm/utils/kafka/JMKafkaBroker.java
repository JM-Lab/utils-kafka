package kr.jm.utils.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kr.jm.utils.enums.OS;
import kr.jm.utils.helper.JMLog;
import kr.jm.utils.helper.JMString;
import kr.jm.utils.helper.JMThread;

/**
 * The Class JMKafkaBroker.
 */
public class JMKafkaBroker extends KafkaServerStartable {
	private static final org.slf4j.Logger log =
			org.slf4j.LoggerFactory.getLogger(JMKafkaBroker.class);
	private ExecutorService kafkaBrokerThreadPool;
	private int port;

	/**
	 * Instantiates a new JM kafka broker.
	 *
	 * @param properties
	 *            the properties
	 */
	public JMKafkaBroker(Properties properties) {
		super(new KafkaConfig(properties));
		this.kafkaBrokerThreadPool = JMThread.newSingleThreadPool();
	}

	/**
	 * Instantiates a new JM kafka broker.
	 *
	 * @param zookeeperConnect
	 *            the zookeeper connect
	 */
	public JMKafkaBroker(String zookeeperConnect) {
		this(zookeeperConnect, OS.getHostname(), OS.getAvailableLocalPort());
	}

	/**
	 * Instantiates a new JM kafka broker.
	 *
	 * @param zookeeperConnect
	 *            the zookeeper connect
	 * @param hostname
	 *            the hostname
	 * @param port
	 *            the port
	 */
	public JMKafkaBroker(String zookeeperConnect, String hostname, int port) {
		this(zookeeperConnect, hostname, port, "kafka-broker-log");
	}

	/**
	 * Instantiates a new JM kafka broker.
	 *
	 * @param zookeeperConnect
	 *            the zookeeper connect
	 * @param hostname
	 *            the hostname
	 * @param port
	 *            the port
	 * @param logDir
	 *            the log dir
	 */
	public JMKafkaBroker(String zookeeperConnect, String hostname, int port,
			String logDir) {
		this(buildProperties(zookeeperConnect, hostname, port, logDir));
		this.port = port;
	}

	/**
	 * Builds the properties.
	 *
	 * @param zookeeperConnect
	 *            the zookeeper connect
	 * @param hostname
	 *            the hostname
	 * @param port
	 *            the port
	 * @param logDir
	 *            the log dir
	 * @return the properties
	 */
	public static Properties buildProperties(String zookeeperConnect,
			String hostname, int port, String logDir) {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", zookeeperConnect);
		properties.put("listeners", "PLAINTEXT://"
				+ JMString.buildIpOrHostnamePortPair(hostname, port));
		properties.put("brokerid", hostname + "-" + System.currentTimeMillis());
		properties.put("log.dir", logDir);
		return properties;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see kafka.server.KafkaServerStartable#startup()
	 */
	@Override
	public void startup() {
		JMThread.runAsync(() -> {
			Thread.currentThread().setName("JMKafkaBroker-" + OS.getHostname());
			JMLog.info(log, "startup");
			super.startup();
		}, kafkaBrokerThreadPool);
	}

	/**
	 * Start.
	 *
	 * @return the JM kafka broker
	 */
	public JMKafkaBroker start() {
		startup();
		return this;
	}

	/**
	 * Stop.
	 */
	public void stop() {
		log.info("shutdown starting ms - " + System.currentTimeMillis());
		kafkaBrokerThreadPool.shutdown();
		shutdown();
		log.info("shutdown completely ms - " + System.currentTimeMillis());
	}

	public int getPort() {
		return port;
	}

}
