package kr.jm.utils.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Optional;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import kr.jm.utils.enums.OS;
import kr.jm.utils.helper.JMPath;
import kr.jm.utils.helper.JMPathOperation;
import kr.jm.utils.helper.JMString;
import kr.jm.utils.helper.JMThread;
import kr.jm.utils.zookeeper.JMZookeeperServer;

public class JMKafkaAdminTest {

	JMKafkaAdmin jmKafkaAdmin;

	private JMZookeeperServer zooKeeper;
	private JMKafkaBroker kafkaBroker;

	@Before
	public void setUp() throws Exception {
		Optional.of(JMPath.getPath("zookeeper-dir")).filter(JMPath::exists)
				.ifPresent(JMPathOperation::deleteDir);
		Optional.of(JMPath.getPath("kafka-broker-log")).filter(JMPath::exists)
				.ifPresent(JMPathOperation::deleteDir);
		this.zooKeeper = new JMZookeeperServer();
		this.zooKeeper.start();
		JMThread.sleep(3000);
		String zookeeperConnect = JMString.buildIpOrHostnamePortPair(
				OS.getHostname(), zooKeeper.getClientPort());
		Properties brokerProperties = JMKafkaBroker.buildProperties(
				zookeeperConnect, OS.getHostname(), OS.getAvailableLocalPort(),
				"kafka-broker-log");
		brokerProperties.setProperty("delete.topic.enable", "true");
		this.kafkaBroker = new JMKafkaBroker(brokerProperties);
		this.kafkaBroker.startup();
		JMThread.sleep(3000);
		this.jmKafkaAdmin = new JMKafkaAdmin(zookeeperConnect,
				kafkaBroker.getBrokerConnect());
	}

	@After
	public void tearDown() throws Exception {
		kafkaBroker.stop();
		zooKeeper.stop();
		Optional.of(JMPath.getPath("zookeeper-dir")).filter(JMPath::exists)
				.ifPresent(JMPathOperation::deleteDir);
		Optional.of(JMPath.getPath("kafka-broker-log")).filter(JMPath::exists)
				.ifPresent(JMPathOperation::deleteDir);
	}

	@Test
	public void testJMKafkaAdmin() throws Exception {
		System.out.println(jmKafkaAdmin.getAllTopicInfo());
		String topic = "newTestTopic";
		jmKafkaAdmin.createTopic(topic, 1, 1);
		System.out.println(jmKafkaAdmin.getAllTopicInfo());
		assertEquals(topic, jmKafkaAdmin.getTopicList().get(0));
		jmKafkaAdmin.createTopic(topic, 1, 1);
		jmKafkaAdmin.createTopic(topic + "2", 1, 1);
		System.out.println(jmKafkaAdmin.getTopicList());
		assertEquals(2, jmKafkaAdmin.getTopicList().size());
		assertEquals(1, jmKafkaAdmin.getPartitionCount(topic));
		System.out.println(jmKafkaAdmin.getPartitionInfo(topic));
		jmKafkaAdmin.deleteTopic(topic);
		System.out.println(jmKafkaAdmin.getAllTopicInfo());
		assertFalse(jmKafkaAdmin.topicExists(topic));
	}

}
