package kr.jm.utils.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kr.jm.utils.exception.JMExceptionManager;

public class KafkaProducer {

	private static final org.slf4j.Logger log =
			org.slf4j.LoggerFactory.getLogger(KafkaProducer.class);

	private Producer<String, String> producer;
	private String topic;
	private ObjectMapper jsonMapper = new ObjectMapper();

	public KafkaProducer(Properties kafkaProducerProperties, String topic) {
		this.topic = topic;
		this.producer = new Producer<String, String>(
				new ProducerConfig(kafkaProducerProperties));
	}

	public void send(KeyedMessage<String, String> keyedMessage) {
		try {
			producer.send(keyedMessage);
		} catch (Exception e) {
			JMExceptionManager.logException(log, e, "send", keyedMessage);
		}
	}

	public void send(List<KeyedMessage<String, String>> keyedMessageList) {
		try {
			producer.send(keyedMessageList);
		} catch (Exception e) {
			JMExceptionManager.logException(log, e, "send", keyedMessageList);
		}
	}

	public void sendJsonStringFromObject(String key, Object object) {
		send(buildKeyedMessage(key, object));
	}

	public int sendJsonStringFromObjectAndGetSendingBytesSize(String key,
			Object object) {
		KeyedMessage<String, String> keyedMessage =
				buildKeyedMessage(key, object);
		send(keyedMessage);
		return keyedMessage.message().getBytes().length;
	}

	public int sendJsonStringFromObjectAndGetSendingBytesSize(Object object) {
		KeyedMessage<String, String> keyedMessage = buildKeyedMessage(object);
		send(keyedMessage);
		return keyedMessage.message().getBytes().length;
	}

	private KeyedMessage<String, String> buildKeyedMessage(String key,
			Object object) {
		return new KeyedMessage<String, String>(topic, key,
				buildJsonString(object));
	}

	private String buildJsonString(Object object) {
		try {
			return jsonMapper.writeValueAsString(object);
		} catch (Exception e) {
			throw JMExceptionManager.handleExceptionAndReturnRuntimeEx(log, e,
					"buildJsonString", object);
		}
	}

	public void sendJsonStringFromObject(Object object) {
		send(buildKeyedMessage(object));
	}

	private KeyedMessage<String, String> buildKeyedMessage(Object object) {
		return new KeyedMessage<String, String>(topic, buildJsonString(object));
	}

	public void sendJsonStringFromObject(String key, List<Object> objectList) {
		List<KeyedMessage<String, String>> keyedMessageList =
				new ArrayList<KeyedMessage<String, String>>();
		for (Object object : objectList)
			keyedMessageList.add(buildKeyedMessage(key, object));
		send(keyedMessageList);
	}

	public void sendJsonStringFromObject(List<Object> objectList) {
		List<KeyedMessage<String, String>> keyedMessageList =
				new ArrayList<KeyedMessage<String, String>>();
		for (Object object : objectList)
			keyedMessageList.add(buildKeyedMessage(object));
		send(keyedMessageList);
	}

	public void close() {
		producer.close();
	}

}
