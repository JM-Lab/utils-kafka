package kr.jm.utils.kafka.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * The type Jm kafka producer.
 */
public class JMKafkaProducer extends KafkaProducer<String, String> {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(JMKafkaProducer.class);
    private String defaultTopic;
    private Properties producerProperties;
    private ObjectMapper objectMapper;

    /**
     * Instantiates a new Jm kafka producer.
     *
     * @param bootstrapServers the bootstrap servers
     */
    public JMKafkaProducer(String bootstrapServers) {
        this(bootstrapServers, null);
    }

    /**
     * Instantiates a new Jm kafka producer.
     *
     * @param bootstrapServers the bootstrap servers
     * @param producerId       the producer id
     */
    public JMKafkaProducer(String bootstrapServers, String producerId) {
        this(bootstrapServers, producerId, 2, 16384, 33554432, 1);
    }

    /**
     * Instantiates a new Jm kafka producer.
     *
     * @param bootstrapServers the bootstrap servers
     * @param producerId       the producer id
     * @param retries          the retries
     * @param batchSize        the batch size
     * @param bufferMemory     the buffer memory
     * @param lingerMs         the linger ms
     */
    public JMKafkaProducer(String bootstrapServers, String producerId,
            int retries, int batchSize, int bufferMemory, int lingerMs) {
        this(buildProperties(bootstrapServers, producerId, retries, batchSize,
                bufferMemory, lingerMs));
    }

    /**
     * Build properties properties.
     *
     * @param bootstrapServers the bootstrap servers
     * @param producerId       the producer id
     * @param retries          the retries
     * @param batchSize        the batch size
     * @param bufferMemory     the buffer memory
     * @param lingerMs         the linger ms
     * @return the properties
     */
    public static Properties buildProperties(String bootstrapServers,
            String producerId, Integer retries, Integer batchSize,
            Integer bufferMemory, Integer lingerMs) {
        return new Properties() {{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    bootstrapServers);
            Optional.ofNullable(producerId).ifPresent(
                    clientId -> put(ProducerConfig.CLIENT_ID_CONFIG, clientId));
            Optional.ofNullable(retries).ifPresent(
                    retries -> put(ProducerConfig.RETRIES_CONFIG, retries));
            Optional.ofNullable(batchSize).ifPresent(
                    batchSize -> put(ProducerConfig.BATCH_SIZE_CONFIG,
                            batchSize));
            Optional.ofNullable(bufferMemory).ifPresent(
                    bufferMemory -> put(ProducerConfig.BUFFER_MEMORY_CONFIG,
                            bufferMemory));
            Optional.ofNullable(lingerMs).ifPresent(
                    lingerMs -> put(ProducerConfig.LINGER_MS_CONFIG, lingerMs));
            put(ProducerConfig.ACKS_CONFIG, "all");
        }};
    }

    /**
     * Instantiates a new Jm kafka producer.
     *
     * @param producerProperties the producer properties
     */
    public JMKafkaProducer(Properties producerProperties) {
        super(producerProperties, Serdes.String().serializer(),
                Serdes.String().serializer());
        this.producerProperties = producerProperties;
        this.objectMapper = new ObjectMapper()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL);
    }


    /**
     * Send list list.
     *
     * @param producerRecordList the producer record list
     * @return the list
     */
    public List<Future<RecordMetadata>>
    sendList(List<ProducerRecord<String, String>> producerRecordList) {
        JMLog.debug(log, "sendList", producerRecordList.size());
        return producerRecordList.stream().map(this::send).collect(toList());
    }

    /**
     * Send future.
     *
     * @param value the value
     * @return the future
     */
    public Future<RecordMetadata> send(String value) {
        return send(null, value);
    }

    /**
     * Send future.
     *
     * @param key   the key
     * @param value the value
     * @return the future
     */
    public Future<RecordMetadata> send(String key, String value) {
        return send(getDefaultTopic(), key, value);
    }

    /**
     * Send future.
     *
     * @param topic the topic
     * @param key   the key
     * @param value the value
     * @return the future
     */
    public Future<RecordMetadata> send(String topic, String key, String value) {
        return send(buildProducerRecord(topic, key, value));
    }

    /**
     * Send string list list.
     *
     * @param value the value
     * @return the list
     */
    public List<Future<RecordMetadata>> sendStringList(List<String> value) {
        return sendStringStream(null, value.stream());
    }

    /**
     * Send string list list.
     *
     * @param key   the key
     * @param value the value
     * @return the list
     */
    public List<Future<RecordMetadata>> sendStringList(String key,
            List<String> value) {
        return sendStringList(getDefaultTopic(), key, value);
    }

    /**
     * Send string list list.
     *
     * @param topic the topic
     * @param key   the key
     * @param value the value
     * @return the list
     */
    public List<Future<RecordMetadata>> sendStringList(String topic, String key,
            List<String> value) {
        return sendStringStream(topic, key, value.stream());
    }

    /**
     * Send string stream list.
     *
     * @param value the value
     * @return the list
     */
    public List<Future<RecordMetadata>> sendStringStream(Stream<String> value) {
        return sendStringStream(getDefaultTopic(), null, value);
    }

    /**
     * Send string stream list.
     *
     * @param key   the key
     * @param value the value
     * @return the list
     */
    public List<Future<RecordMetadata>> sendStringStream(String key,
            Stream<String> value) {
        return sendStringStream(getDefaultTopic(), key, value);
    }

    /**
     * Send string stream list.
     *
     * @param topic the topic
     * @param key   the key
     * @param value the value
     * @return the list
     */
    public List<Future<RecordMetadata>> sendStringStream(String topic,
            String key, Stream<String> value) {
        return value.map(s -> send(topic, key, s)).collect(Collectors.toList());
    }


    /**
     * Send json string future.
     *
     * @param <T>    the type parameter
     * @param object the object
     * @return the future
     */
    public <T> Future<RecordMetadata> sendJsonString(T object) {
        return sendJsonString(null, object);
    }

    /**
     * Send json string future.
     *
     * @param <T>    the type parameter
     * @param key    the key
     * @param object the object
     * @return the future
     */
    public <T> Future<RecordMetadata> sendJsonString(String key, T object) {
        return sendJsonString(getDefaultTopic(), key, object);
    }

    /**
     * Send json string future.
     *
     * @param <T>    the type parameter
     * @param topic  the topic
     * @param key    the key
     * @param object the object
     * @return the future
     */
    public <T> Future<RecordMetadata> sendJsonString(String topic, String key,
            T object) {
        return send(buildProducerRecord(topic, key, object));
    }

    /**
     * Send sync and get serialized size int.
     *
     * @param <T>    the type parameter
     * @param object the object
     * @return the int
     */
    public <T> int sendSyncAndGetSerializedSize(T object) {
        return sendSyncAndGetSerializedSize(null, object);
    }

    /**
     * Send sync and get serialized size int.
     *
     * @param <T>    the type parameter
     * @param key    the key
     * @param object the object
     * @return the int
     */
    public <T> int sendSyncAndGetSerializedSize(String key, T object) {
        return sendSyncAndGetSerializedSize(getDefaultTopic(), key, object);
    }

    /**
     * Send sync and get serialized size int.
     *
     * @param <T>    the type parameter
     * @param topic  the topic
     * @param key    the key
     * @param object the object
     * @return the int
     */
    public <T> int sendSyncAndGetSerializedSize(String topic, String key,
            T object) {
        return sendSyncAndGetSerializedSize(
                buildProducerRecord(topic, key, object));
    }

    /**
     * Send sync optional.
     *
     * @param producerRecord the producer record
     * @return the optional
     */
    public Optional<RecordMetadata>
    sendSync(ProducerRecord<String, String> producerRecord) {
        try {
            JMLog.debug(log, "sendSync", producerRecord);
            return Optional.of(send(producerRecord).get());
        } catch (Exception e) {
            return JMExceptionManager.handleExceptionAndReturnEmptyOptional(log,
                    e, "sendSync", producerRecord);
        }
    }

    /**
     * Send sync optional.
     *
     * @param topic the topic
     * @param key   the key
     * @param value the value
     * @return the optional
     */
    public Optional<RecordMetadata> sendSync(String topic, String key, String
            value) {
        return sendSync(buildProducerRecord(topic, key, value));
    }

    /**
     * Send sync optional.
     *
     * @param key   the key
     * @param value the value
     * @return the optional
     */
    public Optional<RecordMetadata> sendSync(String key, String value) {
        return sendSync(getDefaultTopic(), key, value);
    }

    /**
     * Send sync optional.
     *
     * @param value the value
     * @return the optional
     */
    public Optional<RecordMetadata> sendSync(String value) {
        return sendSync(null, value);
    }

    /**
     * Send list sync list.
     *
     * @param producerRecordList the producer record list
     * @return the list
     */
    public List<Optional<RecordMetadata>> sendListSync(
            List<ProducerRecord<String, String>> producerRecordList) {
        JMLog.debug(log, "sendListSync", producerRecordList.size());
        return producerRecordList.stream().map(this::sendSync)
                .collect(toList());
    }

    private int sendSyncAndGetSerializedSize(
            ProducerRecord<String, String> producerRecord) {
        return sendSync(producerRecord).map(this::buildSentSerializedSize)
                .orElse(0);
    }

    /**
     * Build sent serialized size int.
     *
     * @param recordMetadata the record metadata
     * @return the int
     */
    public int buildSentSerializedSize(RecordMetadata recordMetadata) {
        return recordMetadata.serializedKeySize()
                + recordMetadata.serializedValueSize();
    }


    /**
     * Build producer record producer record.
     *
     * @param topic the topic
     * @param key   the key
     * @param value the value
     * @return the producer record
     */
    public ProducerRecord<String, String> buildProducerRecord(String topic,
            String key, String value) {
        return new ProducerRecord<>(topic, key, value);
    }

    /**
     * Build producer record producer record.
     *
     * @param key   the key
     * @param value the value
     * @return the producer record
     */
    public ProducerRecord<String, String> buildProducerRecord(String key,
            String value) {
        return buildProducerRecord(getDefaultTopic(), key, value);
    }

    /**
     * Build producer record producer record.
     *
     * @param value the value
     * @return the producer record
     */
    public ProducerRecord<String, String> buildProducerRecord(String value) {
        return buildProducerRecord(null, value);
    }

    /**
     * Gets default topic.
     *
     * @return the default topic
     */
    public String getDefaultTopic() {
        return Optional.ofNullable(defaultTopic)
                .orElseGet(() -> this.defaultTopic =
                        "JMKafkaProducer-" + System.currentTimeMillis());
    }


    /**
     * Build producer record producer record.
     *
     * @param <T>    the type parameter
     * @param key    the key
     * @param object the object
     * @return the producer record
     */
    public <T> ProducerRecord<String, String> buildProducerRecord(String key,
            T object) {
        return buildProducerRecord(getDefaultTopic(), key, object);
    }

    /**
     * Build producer record producer record.
     *
     * @param <T>    the type parameter
     * @param topic  the topic
     * @param key    the key
     * @param object the object
     * @return the producer record
     */
    public <T> ProducerRecord<String, String> buildProducerRecord(String topic,
            String key, T object) {
        return buildProducerRecord(topic, key, buildJsonString(object));
    }

    /**
     * Build producer record producer record.
     *
     * @param <T>    the type parameter
     * @param object the object
     * @return the producer record
     */
    public <T> ProducerRecord<String, String> buildProducerRecord(T object) {
        return buildProducerRecord(buildJsonString(object));
    }

    private <T> String buildJsonString(T object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            throw JMExceptionManager.handleExceptionAndReturnRuntimeEx(log, e,
                    "buildJsonString", object);
        }
    }

    /**
     * Send json string sync optional.
     *
     * @param <T>    the type parameter
     * @param object the object
     * @return the optional
     */
    public <T> Optional<RecordMetadata> sendJsonStringSync(T object) {
        return sendJsonStringSync(null, object);
    }

    /**
     * Send json string sync optional.
     *
     * @param <T>    the type parameter
     * @param key    the key
     * @param object the object
     * @return the optional
     */
    public <T> Optional<RecordMetadata> sendJsonStringSync(String key,
            T object) {
        return sendJsonStringSync(getDefaultTopic(), key, object);
    }

    /**
     * Send json string sync optional.
     *
     * @param <T>    the type parameter
     * @param topic  the topic
     * @param key    the key
     * @param object the object
     * @return the optional
     */
    public <T> Optional<RecordMetadata> sendJsonStringSync(String topic,
            String key, T object) {
        return sendSync(buildProducerRecord(topic, key, object));
    }

    /**
     * Send json string list sync list.
     *
     * @param <T>        the type parameter
     * @param topic      the topic
     * @param key        the key
     * @param objectList the object list
     * @return the list
     */
    public <T> List<Optional<RecordMetadata>> sendJsonStringListSync(
            String topic, String key, List<T> objectList) {
        return objectList.stream()
                .map(object -> sendJsonStringSync(topic, key, object))
                .collect(toList());
    }

    /**
     * Send json string list sync list.
     *
     * @param <T>        the type parameter
     * @param key        the key
     * @param objectList the object list
     * @return the list
     */
    public <T> List<Optional<RecordMetadata>> sendJsonStringListSync(String key,
            List<T> objectList) {
        return sendJsonStringListSync(getDefaultTopic(), key, objectList);
    }

    /**
     * Send json string list sync list.
     *
     * @param <T>        the type parameter
     * @param objectList the object list
     * @return the list
     */
    public <T> List<Optional<RecordMetadata>> sendJsonStringListSync(
            List<T> objectList) {
        return sendJsonStringListSync(null, objectList);
    }

    /**
     * Send json string list list.
     *
     * @param <T>        the type parameter
     * @param objectList the object list
     * @return the list
     */
    public <T> List<Future<RecordMetadata>> sendJsonStringList(
            List<T> objectList) {
        return sendJsonStringList(null, objectList);
    }

    /**
     * Send json string list list.
     *
     * @param <T>        the type parameter
     * @param key        the key
     * @param objectList the object list
     * @return the list
     */
    public <T> List<Future<RecordMetadata>> sendJsonStringList(String key,
            List<T> objectList) {
        return sendJsonStringList(getDefaultTopic(), key, objectList);
    }

    /**
     * Send json string list list.
     *
     * @param <T>        the type parameter
     * @param topic      the topic
     * @param key        the key
     * @param objectList the object list
     * @return the list
     */
    public <T> List<Future<RecordMetadata>> sendJsonStringList(String topic,
            String key, List<T> objectList) {
        return sendStringStream(topic, key,
                objectList.stream().map(this::buildJsonString));
    }

    /**
     * With default topic jm kafka producer.
     *
     * @param defaultTopic the default topic
     * @return the jm kafka producer
     */
    public JMKafkaProducer withDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
        return this;
    }

    /**
     * With object mapper jm kafka producer.
     *
     * @param objectMapper the object mapper
     * @return the jm kafka producer
     */
    public JMKafkaProducer withObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        return this;
    }

    /**
     * Gets producer properties.
     *
     * @return the producer properties
     */
    public Properties getProducerProperties() {
        return producerProperties;
    }
}
