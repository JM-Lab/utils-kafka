package kr.jm.utils.kafka.streams;

import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;

/**
 * The Class JMKafkaStreams.
 */
public class JMKafkaStreams extends KafkaStreams {
	private static final org.slf4j.Logger log =
			org.slf4j.LoggerFactory.getLogger(JMKafkaStreams.class);

	/**
	 * Instantiates a new JM kafka streams.
	 *
	 * @param properties
	 *            the properties
	 * @param kStreamBuilder
	 *            the k stream builder
	 */
	public JMKafkaStreams(Properties properties,
			KStreamBuilder kStreamBuilder) {
		super(kStreamBuilder, properties);
		setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(Thread t, Throwable e) {
				JMExceptionManager.logException(log, e, "uncaughtException", t);
			}
		});
	}

	/**
	 * Instantiates a new JM kafka streams.
	 *
	 * @param applicationId
	 *            the application id
	 * @param kafkaBootstrapServers
	 *            the kafka bootstrap servers
	 * @param zookeeperConnect
	 *            the zookeeper connect
	 * @param kStreamBuilder
	 *            the k stream builder
	 */
	public JMKafkaStreams(String applicationId, String kafkaBootstrapServers,
			String zookeeperConnect, KStreamBuilder kStreamBuilder) {
		this(null, applicationId, kafkaBootstrapServers, zookeeperConnect,
				kStreamBuilder);
	}

	/**
	 * Instantiates a new JM kafka streams.
	 *
	 * @param isLatest
	 *            the is latest
	 * @param applicationId
	 *            the application id
	 * @param kafkaBootstrapServers
	 *            the kafka bootstrap servers
	 * @param zookeeperConnect
	 *            the zookeeper connect
	 * @param kStreamBuilder
	 *            the k stream builder
	 */
	public JMKafkaStreams(Boolean isLatest, String applicationId,
			String kafkaBootstrapServers, String zookeeperConnect,
			KStreamBuilder kStreamBuilder) {
		this(buildProperties(isLatest, applicationId, kafkaBootstrapServers,
				zookeeperConnect), kStreamBuilder);
	}

	/**
	 * Builds the properties.
	 *
	 * @param isLatest
	 *            the is latest
	 * @param applicationId
	 *            the application id
	 * @param kafkaBootstrapServers
	 *            the kafka bootstrap servers
	 * @param zookeeperConnect
	 *            the zookeeper connect
	 * @return the properties
	 */
	public static Properties buildProperties(Boolean isLatest,
			String applicationId, String kafkaBootstrapServers,
			String zookeeperConnect) {
		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				kafkaBootstrapServers);
		properties.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,
				zookeeperConnect);
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				Optional.ofNullable(isLatest)
						.map(b -> b ? "latest" : "earliest").orElse("none"));
		properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG,
				Serdes.String().getClass().getName());
		properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG,
				Serdes.String().getClass().getName());
		return properties;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.kafka.streams.KafkaStreams#start()
	 */
	@Override
	public void start() {
		JMLog.info(log, "start");
		super.start();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.kafka.streams.KafkaStreams#close()
	 */
	@Override
	public void close() {
		JMLog.info(log, "close");
		super.close();
	}

}
