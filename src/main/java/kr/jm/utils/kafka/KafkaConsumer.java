package kr.jm.utils.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaConsumer {

	private static final String KAFKA_CONSUMING_WORKER =
			"-kafkaConsumingWorker-";

	private ConsumerConfig consumerConfig;

	private ConsumerConnector kafkaConsumerConnector;

	private Map<String, Integer> topicInfo;

	private ExecutorService kafkaConsumingThreadPool;

	private KafkaStreamWorkInterface kafkaStreamWork;

	public KafkaConsumer(Properties kafkaConsumerProperties,
			Map<String, Integer> topicInfo,
			KafkaStreamWorkInterface kafkaStreamWork) {
		this.consumerConfig = new ConsumerConfig(kafkaConsumerProperties);
		this.topicInfo = topicInfo;
		setKafkaStreamWork(kafkaStreamWork);
	}

	public void start() throws Exception {
		this.kafkaConsumerConnector = kafka.consumer.Consumer
				.createJavaConsumerConnector(consumerConfig);
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams =
				kafkaConsumerConnector.createMessageStreams(topicInfo);
		kafkaConsumingThreadPool =
				Executors.newFixedThreadPool(messageStreams.size());

		for (String topic : topicInfo.keySet()) {
			List<KafkaStream<byte[], byte[]>> kafkaStreamList =
					messageStreams.get(topic);
			log.info(
					"KafkaConsumer For {} Topic Start !!! - Kafka Stream Count = {}",
					topic, kafkaStreamList.size());
			int streamNum = 0;
			for (KafkaStream<byte[], byte[]> kafkaStream : kafkaStreamList) {
				kafkaConsumingThreadPool.submit(
						new ConsumingWorker(topic, ++streamNum, kafkaStream));
				log.info("{}" + KAFKA_CONSUMING_WORKER + "{} Running !!!",
						topic, streamNum);
			}
		}
	}

	public boolean isRunning() {
		return !kafkaConsumingThreadPool.isTerminated();
	}

	public void stop() throws RuntimeException {
		if (kafkaConsumerConnector != null)
			kafkaConsumerConnector.shutdown();
		if (kafkaConsumingThreadPool != null)
			kafkaConsumingThreadPool.shutdown();
		while (!kafkaConsumingThreadPool.isTerminated()) {
		}
	}

	public void setKafkaStreamWork(KafkaStreamWorkInterface kafkaStreamWork) {
		this.kafkaStreamWork = kafkaStreamWork;
	}

	private class ConsumingWorker implements Runnable {

		private String topic;
		private int streamNum;
		private KafkaStream<byte[], byte[]> kafkaStream;
		private String threadName;

		public ConsumingWorker(String topic, int numOfThreadForKafka,
				KafkaStream<byte[], byte[]> kafkaStream) {
			this.topic = topic;
			this.streamNum = numOfThreadForKafka;
			this.kafkaStream = kafkaStream;
			this.threadName = topic + KAFKA_CONSUMING_WORKER + streamNum;
		}

		@Override
		public void run() {
			try {
				Thread.currentThread().setName(threadName);
				kafkaStreamWork.consumingKafkaStream(topic, streamNum,
						kafkaStream);
			} catch (Exception e) {
				log.error("[" + threadName + "] Exception Occur!!!", e);
			} finally {
				log.error("[" + threadName + "] stopped!!!");
			}

		}

	}

}
