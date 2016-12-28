package kr.jm.utils.kafka.client;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;

/**
 * The Class JMKafkaProducer.
 */
public class JMKafkaProducer extends KafkaProducer<String, String> {

	private static final org.slf4j.Logger log =
			org.slf4j.LoggerFactory.getLogger(JMKafkaProducer.class);
	private long timeoutMillis = 1000;
	private String topic;
	private ObjectMapper objectMapper = new ObjectMapper()
			.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
			.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL);

	/**
	 * Instantiates a new JM kafka producer.
	 *
	 * @param properties
	 *            the properties
	 * @param topic
	 *            the topic
	 */
	public JMKafkaProducer(Properties properties, String topic) {
		super(properties, Serdes.String().serializer(),
				Serdes.String().serializer());
		this.topic = topic;
	}

	/**
	 * Instantiates a new JM kafka producer.
	 *
	 * @param bootstrapServers
	 *            the bootstrap servers
	 * @param topic
	 *            the topic
	 */
	public JMKafkaProducer(String bootstrapServers, String topic) {
		this(bootstrapServers, topic, 2);
	}

	/**
	 * Instantiates a new JM kafka producer.
	 *
	 * @param bootstrapServers
	 *            the bootstrap servers
	 * @param topic
	 *            the topic
	 * @param retries
	 *            the retries
	 */
	public JMKafkaProducer(String bootstrapServers, String topic, int retries) {
		this(bootstrapServers, topic, retries, 16384, 33554432);
	}

	/**
	 * Instantiates a new JM kafka producer.
	 *
	 * @param bootstrapServers
	 *            the bootstrap servers
	 * @param topic
	 *            the topic
	 * @param retries
	 *            the retries
	 * @param batchSize
	 *            the batch size
	 * @param bufferMemory
	 *            the buffer memory
	 */
	public JMKafkaProducer(String bootstrapServers, String topic, int retries,
			int batchSize, int bufferMemory) {
		this(buildProperties(bootstrapServers, retries, batchSize,
				bufferMemory), topic);
	}

	/**
	 * Builds the properties.
	 *
	 * @param bootstrapServers
	 *            the bootstrap servers
	 * @param retries
	 *            the retries
	 * @param batchSize
	 *            the batch size
	 * @param bufferMemory
	 *            the buffer memory
	 * @return the properties
	 */
	public static Properties buildProperties(String bootstrapServers,
			int retries, int batchSize, int bufferMemory) {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				bootstrapServers);
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(ProducerConfig.RETRIES_CONFIG, retries);
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
		properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
		return properties;
	}

	/**
	 * Send list.
	 *
	 * @param producerRecordList
	 *            the producer record list
	 * @return the list
	 */
	public List<Future<RecordMetadata>>
			sendList(List<ProducerRecord<String, String>> producerRecordList) {
		JMLog.debug(log, "sendList", producerRecordList.size());
		return producerRecordList.stream().map(super::send).collect(toList());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.kafka.clients.producer.KafkaProducer#send(org.apache.kafka.
	 * clients.producer.ProducerRecord,
	 * org.apache.kafka.clients.producer.Callback)
	 */
	@Override
	public Future<RecordMetadata> send(ProducerRecord<String, String> record,
			Callback callback) {
		JMLog.debug(log, "send", record, callback);
		return super.send(record, callback);
	}

	/**
	 * Send.
	 *
	 * @param key
	 *            the key
	 * @param value
	 *            the value
	 * @return the future
	 */
	public Future<RecordMetadata> send(String key, String value) {
		return send(buildProducerRecord(key, value));
	}

	/**
	 * Send.
	 *
	 * @param value
	 *            the value
	 * @return the future
	 */
	public Future<RecordMetadata> send(String value) {
		return send(buildProducerRecord(value));
	}

	/**
	 * Send json string.
	 *
	 * @param <T>
	 *            the generic type
	 * @param object
	 *            the object
	 * @return the future
	 */
	public <T> Future<RecordMetadata> sendJsonString(T object) {
		return send(buildProducerRecord(object));
	}

	/**
	 * Send json string.
	 *
	 * @param <T>
	 *            the generic type
	 * @param key
	 *            the key
	 * @param object
	 *            the object
	 * @return the future
	 */
	public <T> Future<RecordMetadata> sendJsonString(String key, T object) {
		return send(buildProducerRecord(key, object));
	}

	/**
	 * Send sync and get serialized size.
	 *
	 * @param <T>
	 *            the generic type
	 * @param key
	 *            the key
	 * @param object
	 *            the object
	 * @return the int
	 */
	public <T> int sendSyncAndGetSerializedSize(String key, T object) {
		return sendSyncAndGetSerializedSize(buildProducerRecord(key, object));
	}

	/**
	 * Send sync.
	 *
	 * @param producerRecord
	 *            the producer record
	 * @return the optional
	 */
	public Optional<RecordMetadata>
			sendSync(ProducerRecord<String, String> producerRecord) {
		try {
			JMLog.debug(log, "sendSync", producerRecord);
			return Optional.of(super.send(producerRecord, null)
					.get(timeoutMillis, TimeUnit.MILLISECONDS));
		} catch (Exception e) {
			return JMExceptionManager.handleExceptionAndReturnEmptyOptional(log,
					e, "sendSync", producerRecord);
		}
	}

	/**
	 * Send sync.
	 *
	 * @param key
	 *            the key
	 * @param value
	 *            the value
	 * @return the optional
	 */
	public Optional<RecordMetadata> sendSync(String key, String value) {
		return sendSync(buildProducerRecord(key, value));
	}

	/**
	 * Send sync.
	 *
	 * @param value
	 *            the value
	 * @return the optional
	 */
	public Optional<RecordMetadata> sendSync(String value) {
		return sendSync(buildProducerRecord(value));
	}

	/**
	 * Send list sync.
	 *
	 * @param producerRecordList
	 *            the producer record list
	 * @return the list
	 */
	public List<Optional<RecordMetadata>> sendListSync(
			List<ProducerRecord<String, String>> producerRecordList) {
		JMLog.debug(log, "sendListSync", producerRecordList.size());
		return producerRecordList.stream().map(this::sendSync)
				.collect(toList());
	}

	/**
	 * Send sync and get serialized size.
	 *
	 * @param <T>
	 *            the generic type
	 * @param object
	 *            the object
	 * @return the int
	 */
	public <T> int sendSyncAndGetSerializedSize(T object) {
		return sendSyncAndGetSerializedSize(buildProducerRecord(object));
	}

	private int sendSyncAndGetSerializedSize(
			ProducerRecord<String, String> producerRecord) {
		return sendSync(producerRecord).map(this::buildSendedSerializedSize)
				.orElse(0);
	}

	/**
	 * Builds the sended serialized size.
	 *
	 * @param recordMetadata
	 *            the record metadata
	 * @return the int
	 */
	public int buildSendedSerializedSize(RecordMetadata recordMetadata) {
		return recordMetadata.serializedKeySize()
				+ recordMetadata.serializedValueSize();
	}

	/**
	 * Builds the producer record.
	 *
	 * @param key
	 *            the key
	 * @param value
	 *            the value
	 * @return the producer record
	 */
	public ProducerRecord<String, String> buildProducerRecord(String key,
			String value) {
		return new ProducerRecord<>(topic, key, value);
	}

	/**
	 * Builds the producer record.
	 *
	 * @param value
	 *            the value
	 * @return the producer record
	 */
	public ProducerRecord<String, String> buildProducerRecord(String value) {
		return new ProducerRecord<>(topic, value);
	}

	/**
	 * Builds the producer record.
	 *
	 * @param <T>
	 *            the generic type
	 * @param key
	 *            the key
	 * @param object
	 *            the object
	 * @return the producer record
	 */
	public <T> ProducerRecord<String, String> buildProducerRecord(String key,
			T object) {
		return buildProducerRecord(key, buildJsonString(object));
	}

	/**
	 * Builds the producer record.
	 *
	 * @param <T>
	 *            the generic type
	 * @param object
	 *            the object
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
	 * Send json string sync.
	 *
	 * @param <T>
	 *            the generic type
	 * @param object
	 *            the object
	 * @return the optional
	 */
	public <T> Optional<RecordMetadata> sendJsonStringSync(T object) {
		return sendSync(buildProducerRecord(object));
	}

	/**
	 * Send json string sync.
	 *
	 * @param <T>
	 *            the generic type
	 * @param key
	 *            the key
	 * @param object
	 *            the object
	 * @return the optional
	 */
	public <T> Optional<RecordMetadata> sendJsonStringSync(String key,
			T object) {
		return sendSync(buildProducerRecord(key, object));
	}

	/**
	 * Send json string list sync.
	 *
	 * @param <T>
	 *            the generic type
	 * @param key
	 *            the key
	 * @param objectList
	 *            the object list
	 * @return the list
	 */
	public <T> List<Optional<RecordMetadata>> sendJsonStringListSync(String key,
			List<T> objectList) {
		return sendListSync(objectList.stream()
				.map(object -> buildProducerRecord(key, object))
				.collect(toList()));
	}

	/**
	 * Send json string list sync.
	 *
	 * @param <T>
	 *            the generic type
	 * @param objectList
	 *            the object list
	 * @return the list
	 */
	public <T> List<Optional<RecordMetadata>>
			sendJsonStringListSync(List<T> objectList) {
		return sendListSync(objectList.stream()
				.map(object -> buildProducerRecord(object)).collect(toList()));
	}

	/**
	 * Send json string list.
	 *
	 * @param <T>
	 *            the generic type
	 * @param key
	 *            the key
	 * @param objectList
	 *            the object list
	 * @return the list
	 */
	public <T> List<Future<RecordMetadata>> sendJsonStringList(String key,
			List<T> objectList) {
		return sendList(objectList.stream()
				.map(object -> buildProducerRecord(key, object))
				.collect(toList()));
	}

	/**
	 * Send json string list.
	 *
	 * @param <T>
	 *            the generic type
	 * @param objectList
	 *            the object list
	 * @return the list
	 */
	public <T> List<Future<RecordMetadata>>
			sendJsonStringList(List<T> objectList) {
		return sendList(objectList.stream()
				.map(object -> buildProducerRecord(object)).collect(toList()));
	}

	public long getTimeoutMillis() {
		return timeoutMillis;
	}

	public void setTimeoutMillis(long timeoutMillis) {
		this.timeoutMillis = timeoutMillis;
	}

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

}
