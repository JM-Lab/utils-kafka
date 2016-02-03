package kr.jm.utils.kafka;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class KafkaConsumerTest {

	private static final Integer numOfKafkaPartition = 1;
	private String zookeeper = "localhost:2181";
	private String groupId = "test";
	private String topic = "collectd";

	private Map<String, Integer> topicInfo;
	private KafkaConsumer kafkaConsumer;

	@Before
	public void setUp() throws Exception {
		this.topicInfo = new HashMap<String, Integer>();
		this.topicInfo.put(topic, numOfKafkaPartition);
		Properties zookeeperProperties = new Properties();
		zookeeperProperties.put("zookeeper.connect", zookeeper);
		zookeeperProperties.put("group.id", groupId);
		zookeeperProperties.put("auto.offset.reset", "smallest");
		zookeeperProperties.put("zookeeper.session.timeout.ms", "5000");
		zookeeperProperties.put("zookeeper.sync.time.ms", "200");
		zookeeperProperties.put("auto.commit.interval.ms", "1000");
		KafkaStreamConsumerInterface kafkaStreamConsumer =
				new KafkaStreamConsumerInterface() {
					@Override
					public void consume(byte[] message) {
						throw new NullPointerException(new String(message));
					}
				};
		this.kafkaConsumer = new KafkaConsumer(zookeeperProperties, topicInfo,
				kafkaStreamConsumer);
	}

	@Ignore
	@Test
	public final void testStart() throws Exception {
		kafkaConsumer.start();
		assertTrue(kafkaConsumer.isRunning());
		Thread.sleep(1000);
		assertTrue(kafkaConsumer.isRunning());
		Thread.sleep(1000);
		assertTrue(kafkaConsumer.isRunning());
		kafkaConsumer.stop();
		Thread.sleep(1000);
		assertTrue(!kafkaConsumer.isRunning());
	}

}
