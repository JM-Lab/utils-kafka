package kr.jm.utils.kafka;

import kafka.consumer.KafkaStream;

public interface KafkaStreamWorkInterface {

	public void consumingKafkaStream(String topic, int streamNum,
			KafkaStream<byte[], byte[]> kafkaStream);
}