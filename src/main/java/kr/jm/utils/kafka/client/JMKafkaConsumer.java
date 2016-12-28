package kr.jm.utils.kafka.client;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;

import kr.jm.utils.datastructure.JMCollections;
import kr.jm.utils.enums.OS;
import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;
import kr.jm.utils.helper.JMThread;

/**
 * The Class JMKafkaConsumer.
 */
public class JMKafkaConsumer extends KafkaConsumer<String, String> {

	private static final org.slf4j.Logger log =
			org.slf4j.LoggerFactory.getLogger(JMKafkaConsumer.class);

	private final AtomicBoolean isRunning = new AtomicBoolean();
	private final AtomicBoolean isPaused = new AtomicBoolean();
	private int pollIntervalMs = 100;
	private String[] topics;

	private String groupId;

	private ExecutorService kafkaConsumerThreadPool;

	private RecordsConsumer consumer;

	/**
	 * Instantiates a new JM kafka consumer.
	 *
	 * @param properties
	 *            the properties
	 * @param consumer
	 *            the consumer
	 * @param topics
	 *            the topics
	 */
	public JMKafkaConsumer(Properties properties, RecordsConsumer consumer,
			String... topics) {
		super(properties, Serdes.String().deserializer(),
				Serdes.String().deserializer());
		this.kafkaConsumerThreadPool = JMThread.newSingleThreadPool();
		this.consumer = consumer;
		this.topics = topics;
		this.groupId = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
	}

	/**
	 * Instantiates a new JM kafka consumer.
	 *
	 * @param bootstrapServers
	 *            the bootstrap servers
	 * @param groupId
	 *            the group id
	 * @param consumer
	 *            the consumer
	 * @param topics
	 *            the topics
	 */
	public JMKafkaConsumer(String bootstrapServers, String groupId,
			RecordsConsumer consumer, String... topics) {
		this(null, bootstrapServers, groupId, consumer, topics);
	}

	/**
	 * Instantiates a new JM kafka consumer.
	 *
	 * @param isLatest
	 *            the is latest
	 * @param bootstrapServers
	 *            the bootstrap servers
	 * @param groupId
	 *            the group id
	 * @param consumer
	 *            the consumer
	 * @param topics
	 *            the topics
	 */
	public JMKafkaConsumer(Boolean isLatest, String bootstrapServers,
			String groupId, RecordsConsumer consumer, String... topics) {
		this(isLatest, bootstrapServers, groupId, 1000, 30000, consumer,
				topics);
	}

	/**
	 * Instantiates a new JM kafka consumer.
	 *
	 * @param isLatest
	 *            the is latest
	 * @param bootstrapServers
	 *            the bootstrap servers
	 * @param groupId
	 *            the group id
	 * @param autoCommitIntervalMs
	 *            the auto commit interval ms
	 * @param sessionTimeoutMs
	 *            the session timeout ms
	 * @param consumer
	 *            the consumer
	 * @param topics
	 *            the topics
	 */
	public JMKafkaConsumer(Boolean isLatest, String bootstrapServers,
			String groupId, int autoCommitIntervalMs, int sessionTimeoutMs,
			RecordsConsumer consumer, String... topics) {
		this(buildProperties(isLatest, bootstrapServers, groupId,
				autoCommitIntervalMs, sessionTimeoutMs), consumer, topics);
	}

	/**
	 * Builds the properties.
	 *
	 * @param isLatest
	 *            the is latest
	 * @param bootstrapServers
	 *            the bootstrap servers
	 * @param groupId
	 *            the group id
	 * @param autoCommitIntervalMs
	 *            the auto commit interval ms
	 * @param sessionTimeoutMs
	 *            the session timeout ms
	 * @return the properties
	 */
	public static Properties buildProperties(Boolean isLatest,
			String bootstrapServers, String groupId, int autoCommitIntervalMs,
			int sessionTimeoutMs) {
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				bootstrapServers);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
				autoCommitIntervalMs);
		properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
				sessionTimeoutMs);
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				Optional.ofNullable(isLatest)
						.map(b -> b ? "latest" : "earliest").orElse("none"));
		return properties;
	}

	/**
	 * Subscribe.
	 *
	 * @param topics
	 *            the topics
	 */
	public void subscribe(String... topics) {
		super.subscribe(Arrays.asList(topics));
	}

	/**
	 * Start.
	 */
	public void start() {
		JMLog.info(log, "start", groupId, getTopicList(), pollIntervalMs);
		JMThread.runAsync(this::consume, kafkaConsumerThreadPool);
		isRunning.set(true);
	}

	private void consume() {
		try {
			Thread.currentThread().setName(
					"JMKafkaConsumer-" + OS.getHostname() + "-" + groupId);
			subscribe(topics);
			while (isRunning()) {
				handleConsumerRecords(poll(pollIntervalMs));
				checkPauseStatus();
			}
		} catch (WakeupException e) {
			if (isRunning())
				JMExceptionManager.logException(log, e, "consume");
		} finally {
			super.close();
		}
	}

	private void handleConsumerRecords(
			ConsumerRecords<String, String> consumerRecords) {
		log.debug("Consume Timestamp = {}, Record Count = {}",
				System.currentTimeMillis(), consumerRecords.count());
		consumer.accept(consumerRecords);
	}

	private void checkPauseStatus() {
		while (isPaused())
			JMThread.sleep(100);
	}

	public boolean isRunning() {
		return this.isRunning.get();
	}

	public void setPaused(boolean isPaused) {
		JMLog.info(log, "setPaused", isPaused);
		this.isPaused.set(isPaused);
	}

	public boolean isPaused() {
		return this.isPaused.get();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.kafka.clients.consumer.KafkaConsumer#close()
	 */
	@Override
	public void close() {
		kafkaConsumerThreadPool.shutdown();
		isRunning.set(false);
		wakeup();
		while (!kafkaConsumerThreadPool.isTerminated());
	}

	public int getPollIntervalMs() {
		return pollIntervalMs;
	}

	public void setPollIntervalMs(int pollIntervalMs) {
		this.pollIntervalMs = pollIntervalMs;
	}

	public List<String> getTopicList() {
		return JMCollections.buildList(topics);
	}

	public String getGroupId() {
		return groupId;
	}

	/**
	 * The Interface RecordsConsumer.
	 */
	@FunctionalInterface
	public interface RecordsConsumer
			extends Consumer<ConsumerRecords<String, String>> {
	}

}
