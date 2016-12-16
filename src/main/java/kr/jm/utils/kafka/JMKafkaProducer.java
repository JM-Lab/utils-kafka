package kr.jm.utils.kafka;

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

import com.fasterxml.jackson.databind.ObjectMapper;

import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;

public class JMKafkaProducer extends KafkaProducer<String, String> {

	private static final org.slf4j.Logger log =
			org.slf4j.LoggerFactory.getLogger(JMKafkaProducer.class);
	private long timeoutMillis = 1000;
	private String topic;
	private ObjectMapper jsonMapper = new ObjectMapper();

	public JMKafkaProducer(Properties properties, String topic) {
		super(properties, Serdes.String().serializer(),
				Serdes.String().serializer());
		this.topic = topic;
	}

	public JMKafkaProducer(String bootstrapServers, String topic) {
		this(bootstrapServers, topic, 2);
	}

	public JMKafkaProducer(String bootstrapServers, String topic, int retries) {
		this(bootstrapServers, topic, retries, 16384, 33554432);
	}

	public JMKafkaProducer(String bootstrapServers, String topic, int retries,
			int batchSize, int bufferMemory) {
		this(buildProperties(bootstrapServers, retries, batchSize,
				bufferMemory), topic);
	}

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

	public List<Future<RecordMetadata>>
			sendList(List<ProducerRecord<String, String>> producerRecordList) {
		JMLog.debug(log, "sendList", producerRecordList.size());
		return producerRecordList.stream().map(super::send).collect(toList());
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<String, String> record) {
		JMLog.debug(log, "send", record);
		return super.send(record);
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<String, String> record,
			Callback callback) {
		JMLog.debug(log, "send", record, callback);
		return super.send(record, callback);
	}

	public Future<RecordMetadata> send(String key, String value) {
		JMLog.debug(log, "send", key, value);
		return send(buildProducerRecord(key, value));
	}

	public Future<RecordMetadata> send(String value) {
		JMLog.debug(log, "send", value);
		return send(buildProducerRecord(value));
	}

	public <T> Future<RecordMetadata> sendJsonString(String key, T object) {
		return send(buildProducerRecord(key, object));
	}

	public <T> int sendSyncAndGetSerializedSize(String key, T object) {
		return sendSyncAndGetSerializedSize(buildProducerRecord(key, object));
	}

	public Optional<RecordMetadata>
			sendSync(ProducerRecord<String, String> producerRecord) {
		try {
			return Optional.of(send(producerRecord).get(timeoutMillis,
					TimeUnit.MILLISECONDS));
		} catch (Exception e) {
			return JMExceptionManager.handleExceptionAndReturnEmptyOptional(log,
					e, "sendSync", producerRecord);
		}
	}

	public Optional<RecordMetadata> sendSync(String key, String value) {
		JMLog.debug(log, "sendSync", key, value);
		return sendSync(buildProducerRecord(key, value));
	}

	public Optional<RecordMetadata> sendSync(String value) {
		JMLog.debug(log, "sendSync", value);
		return sendSync(buildProducerRecord(value));
	}

	public List<Optional<RecordMetadata>> sendListSync(
			List<ProducerRecord<String, String>> producerRecordList) {
		JMLog.debug(log, "sendListSync", producerRecordList.size());
		return producerRecordList.stream().map(this::sendSync)
				.collect(toList());
	}

	public <T> int sendSyncAndGetSerializedSize(T object) {
		return sendSyncAndGetSerializedSize(buildProducerRecord(object));
	}

	private int sendSyncAndGetSerializedSize(
			ProducerRecord<String, String> producerRecord) {
		return sendSync(producerRecord).map(this::buildSendedSerializedSize)
				.orElse(0);
	}

	public int buildSendedSerializedSize(RecordMetadata recordMetadata) {
		return recordMetadata.serializedKeySize()
				+ recordMetadata.serializedValueSize();
	}

	public ProducerRecord<String, String> buildProducerRecord(String key,
			String value) {
		return new ProducerRecord<>(topic, key, value);
	}

	public ProducerRecord<String, String> buildProducerRecord(String value) {
		return new ProducerRecord<>(topic, value);
	}

	public <T> ProducerRecord<String, String> buildProducerRecord(String key,
			T object) {
		return buildProducerRecord(key, buildJsonString(object));
	}

	public <T> ProducerRecord<String, String> buildProducerRecord(T object) {
		return buildProducerRecord(buildJsonString(object));
	}

	private <T> String buildJsonString(T object) {
		try {
			return jsonMapper.writeValueAsString(object);
		} catch (Exception e) {
			throw JMExceptionManager.handleExceptionAndReturnRuntimeEx(log, e,
					"buildJsonString", object);
		}
	}

	public <T> Optional<RecordMetadata> sendJsonStringSync(T object) {
		return sendSync(buildProducerRecord(object));
	}

	public <T> List<Optional<RecordMetadata>> sendJsonStringListSync(String key,
			List<T> objectList) {
		return sendListSync(objectList.stream()
				.map(object -> buildProducerRecord(key, object))
				.collect(toList()));
	}

	public <T> List<Optional<RecordMetadata>>
			sendJsonStringListSync(List<T> objectList) {
		return sendListSync(objectList.stream()
				.map(object -> buildProducerRecord(object)).collect(toList()));
	}

	public <T> List<Future<RecordMetadata>> sendJsonStringList(String key,
			List<T> objectList) {
		return sendList(objectList.stream()
				.map(object -> buildProducerRecord(key, object))
				.collect(toList()));
	}

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

}
