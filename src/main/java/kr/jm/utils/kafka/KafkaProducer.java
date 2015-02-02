package kr.jm.utils.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaProducer {
	private Producer<String, String> producer;
	private String topic;
	private ObjectMapper jsonMapper = new ObjectMapper();

	public KafkaProducer(Properties kafkaProducerProperties, String topic) {
		this.topic = topic;
		this.producer = new Producer<String, String>(new ProducerConfig(
				kafkaProducerProperties));
	}

	public void sendJsonStringFromObject(String key, Object object)
			throws Exception {
		producer.send(buildKeyedMessage(key, object));
	}

	private KeyedMessage<String, String> buildKeyedMessage(String key,
			Object object) throws JsonProcessingException {
		return new KeyedMessage<String, String>(topic, key,
				jsonMapper.writeValueAsString(object));
	}

	public void sendJsonStringFromObject(Object object) throws Exception {
		producer.send(buildKeyedMessage(object));
	}

	private KeyedMessage<String, String> buildKeyedMessage(Object object)
			throws JsonProcessingException {
		return new KeyedMessage<String, String>(topic,
				jsonMapper.writeValueAsString(object));
	}

	public void sendJsonStringFromObject(String key, List<Object> objectList)
			throws Exception {
		List<KeyedMessage<String, String>> keyedMessageList = new ArrayList<KeyedMessage<String, String>>();
		for (Object object : objectList)
			keyedMessageList.add(buildKeyedMessage(key, object));
		producer.send(keyedMessageList);
	}

	public void sendJsonStringFromObject(List<Object> objectList)
			throws Exception {
		List<KeyedMessage<String, String>> keyedMessageList = new ArrayList<KeyedMessage<String, String>>();
		for (Object object : objectList)
			keyedMessageList.add(buildKeyedMessage(object));
		producer.send(keyedMessageList);
	}

	public void close() {
		producer.close();
	}

}
