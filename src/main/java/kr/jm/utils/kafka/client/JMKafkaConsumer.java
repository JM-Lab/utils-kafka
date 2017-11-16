package kr.jm.utils.kafka.client;

import kr.jm.utils.datastructure.JMCollections;
import kr.jm.utils.enums.OS;
import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;
import kr.jm.utils.helper.JMThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * The Class JMKafkaConsumer.
 */
public class JMKafkaConsumer extends KafkaConsumer<String, String> {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(JMKafkaConsumer.class);

    private volatile AtomicBoolean closed;
    private volatile AtomicBoolean paused;
    private int pollIntervalMs;
    private String[] topics;

    private String groupId;

    private ExecutorService kafkaConsumerThreadPool;

    private RecordsConsumer consumer;

    /**
     * Instantiates a new JM kafka consumer.
     *
     * @param properties the properties
     * @param consumer   the consumer
     * @param topics     the topics
     */
    public JMKafkaConsumer(Properties properties, RecordsConsumer consumer,
            String... topics) {
        super(properties, Serdes.String().deserializer(),
                Serdes.String().deserializer());
        this.closed = new AtomicBoolean(true);
        this.paused = new AtomicBoolean();
        this.pollIntervalMs = 100;
        this.kafkaConsumerThreadPool = JMThread.newSingleThreadPool();
        this.consumer = consumer;
        this.topics = topics;
        this.groupId = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        subscribe(topics);
    }

    /**
     * Instantiates a new JM kafka consumer.
     *
     * @param bootstrapServers the bootstrap servers
     * @param groupId          the group id
     * @param consumer         the consumer
     * @param topics           the topics
     */
    public JMKafkaConsumer(String bootstrapServers, String groupId,
            RecordsConsumer consumer, String... topics) {
        this(false, bootstrapServers, groupId, consumer, topics);
    }

    /**
     * Instantiates a new JM kafka consumer.
     *
     * @param isLatest         the is latest
     * @param bootstrapServers the bootstrap servers
     * @param groupId          the group id
     * @param consumer         the consumer
     * @param topics           the topics
     */
    public JMKafkaConsumer(boolean isLatest, String bootstrapServers,
            String groupId, RecordsConsumer consumer, String... topics) {
        this(isLatest, bootstrapServers, groupId, 1000, consumer,
                topics);
    }

    /**
     * Instantiates a new JM kafka consumer.
     *
     * @param isLatest             the is latest
     * @param bootstrapServers     the bootstrap servers
     * @param groupId              the group id
     * @param autoCommitIntervalMs the auto commit interval ms
     * @param consumer             the consumer
     * @param topics               the topics
     */
    public JMKafkaConsumer(boolean isLatest, String bootstrapServers,
            String groupId, int autoCommitIntervalMs, RecordsConsumer consumer,
            String... topics) {
        this(new Properties() {{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    bootstrapServers);
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                    autoCommitIntervalMs);
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                    isLatest ? "latest" : "earliest");
        }}, consumer, topics);
    }

    /**
     * Subscribe.
     *
     * @param topics the topics
     */
    public void subscribe(String... topics) {
        subscribe(Arrays.asList(topics));
    }

    /**
     * Start.
     *
     * @return the jm kafka consumer
     */
    public JMKafkaConsumer start() {
        JMLog.info(log, "start", groupId, Arrays.asList(topics),
                pollIntervalMs);
        JMThread.runAsync(this::consume, kafkaConsumerThreadPool);
        return this;
    }

    private void consume() {
        try {
            Thread.currentThread().setName(
                    "JMKafkaConsumer-" + OS.getHostname() + "-" + groupId);
            closed.set(false);
            while (isRunning()) {
                handleConsumerRecords(poll(pollIntervalMs));
                checkPauseStatus();
            }
        } catch (Exception e) {
            if (isRunning())
                JMExceptionManager.handleExceptionAndThrowRuntimeEx(log, e,
                        "consume#WakeupException");
        } finally {
            close();
        }
    }

    private void handleConsumerRecords(
            ConsumerRecords<String, String> consumerRecords) {
        log.debug("Consume Timestamp = {}, Record Count = {}",
                System.currentTimeMillis(), consumerRecords.count());
        try {
            consumer.accept(consumerRecords);
        } catch (Exception e) {
            JMExceptionManager.logException(log, e, "handleConsumerRecords",
                    consumerRecords);
        }
    }

    private void checkPauseStatus() {
        while (isPaused())
            JMThread.sleep(100);
    }

    /**
     * Is running boolean.
     *
     * @return the boolean
     */
    public boolean isRunning() {
        return !this.closed.get();
    }

    /**
     * Is paused boolean.
     *
     * @return the boolean
     */
    public boolean isPaused() {
        return this.paused.get();
    }

    /**
     * Sets paused.
     *
     * @param isPaused the is paused
     */
    public void setPaused(boolean isPaused) {
        JMLog.info(log, "setPaused", isPaused);
        this.paused.set(isPaused);
    }

    /**
     * Shutdown.
     */
    public void shutdown() {
        this.closed.set(true);
        wakeup();
        kafkaConsumerThreadPool.shutdown();
        while (!kafkaConsumerThreadPool.isTerminated())
            JMThread.sleep(100);
    }

    /**
     * Gets poll interval ms.
     *
     * @return the poll interval ms
     */
    public int getPollIntervalMs() {
        return pollIntervalMs;
    }

    /**
     * Sets poll interval ms.
     *
     * @param pollIntervalMs the poll interval ms
     */
    public void setPollIntervalMs(int pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
    }

    /**
     * Gets topic list.
     *
     * @return the topic list
     */
    public List<String> getTopicList() {
        return JMCollections.buildList(topics);
    }

    /**
     * Gets group id.
     *
     * @return the group id
     */
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
