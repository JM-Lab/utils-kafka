package kr.jm.utils.kafka;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;

import kr.jm.utils.enums.OS;
import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;
import kr.jm.utils.helper.JMThread;

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

	public JMKafkaConsumer(Properties properties, RecordsConsumer consumer,
			String... topics) {
		super(properties, Serdes.String().deserializer(),
				Serdes.String().deserializer());
		this.kafkaConsumerThreadPool = JMThread.newSingleThreadPool();
		this.consumer = consumer;
		this.topics = topics;
		this.groupId = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
	}

	public JMKafkaConsumer(String bootstrapServers, String groupId,
			RecordsConsumer consumer, String... topics) {
		this(bootstrapServers, true, groupId, consumer, topics);
	}

	public JMKafkaConsumer(String bootstrapServers, boolean isLatest,
			String groupId, RecordsConsumer consumer, String... topics) {
		this(bootstrapServers, isLatest, groupId, 1000, 30000, consumer,
				topics);
	}

	public JMKafkaConsumer(String bootstrapServers, boolean isLatest,
			String groupId, int autoCommitIntervalMs, int sessionTimeoutMs,
			RecordsConsumer consumer, String... topics) {
		this(buildProperties(bootstrapServers, isLatest, groupId,
				autoCommitIntervalMs, sessionTimeoutMs), consumer, topics);
	}

	public static Properties buildProperties(String bootstrapServers,
			boolean isLatest, String groupId, int autoCommitIntervalMs,
			int sessionTimeoutMs) {
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				bootstrapServers);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
				autoCommitIntervalMs);
		properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
				sessionTimeoutMs);
		if (!isLatest)
			properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return properties;
	}

	public void subscribe(String... topics) {
		super.subscribe(Arrays.asList(topics));
	}

	public void run() {
		JMLog.info(log, "run", topics, pollIntervalMs);
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

	@FunctionalInterface
	public interface RecordsConsumer
			extends Consumer<ConsumerRecords<String, String>> {
	}

}
