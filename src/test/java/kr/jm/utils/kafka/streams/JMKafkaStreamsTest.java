package kr.jm.utils.kafka.streams;

import static java.util.stream.Collectors.toMap;
import static kr.jm.utils.helper.JMLambda.getSelf;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;

import kr.jm.utils.enums.OS;
import kr.jm.utils.helper.JMPath;
import kr.jm.utils.helper.JMPathOperation;
import kr.jm.utils.helper.JMStream;
import kr.jm.utils.helper.JMString;
import kr.jm.utils.helper.JMThread;
import kr.jm.utils.kafka.JMKafkaBroker;
import kr.jm.utils.kafka.client.JMKafkaProducer;
import kr.jm.utils.zookeeper.JMZookeeperServer;

/**
 * The Class JMKafkaStreamsTest.
 */
public class JMKafkaStreamsTest {

	private String topic = "testStreamLocal";
	private JMZookeeperServer zooKeeper;
	private JMKafkaBroker kafkaBroker;
	private JMKafkaProducer kafkaProducer;
	private String bootstrapServer;
	private String zookeeperConnect;

	private String applicationId = "testKafkaStream";
	private JMKafkaStreams jmKafkaStreams;

	public JMKafkaStreamsTest() {
		Optional.of(JMPath.getPath("zookeeper-dir")).filter(JMPath::exists)
				.ifPresent(JMPathOperation::deleteDir);
		Optional.of(JMPath.getPath("kafka-broker-log")).filter(JMPath::exists)
				.ifPresent(JMPathOperation::deleteDir);
		JMThread.sleep(1000);
	}

	/**
	 * Sets the up.
	 *
	 * @throws Exception
	 *             the exception
	 */
	@Before
	public void setUp() throws Exception {
		Optional.of(JMPath.getPath("zookeeper-dir")).filter(JMPath::exists)
				.ifPresent(JMPathOperation::deleteDir);
		Optional.of(JMPath.getPath("kafka-broker-log")).filter(JMPath::exists)
				.ifPresent(JMPathOperation::deleteDir);
		this.zooKeeper = new JMZookeeperServer().start();
		JMThread.sleep(5000);
		zookeeperConnect = JMString.buildIpOrHostnamePortPair(OS.getHostname(),
				zooKeeper.getClientPort());
		this.kafkaBroker = new JMKafkaBroker(zookeeperConnect).start();
		JMThread.sleep(5000);
		bootstrapServer = JMString.buildIpOrHostnamePortPair(OS.getHostname(),
				kafkaBroker.getPort());
		this.kafkaProducer = new JMKafkaProducer(bootstrapServer, topic);
		JMThread.sleep(3000);
	}

	/**
	 * Tear down.
	 *
	 * @throws Exception
	 *             the exception
	 */
	@After
	public void tearDown() throws Exception {
		kafkaProducer.close();
		jmKafkaStreams.close();
		kafkaBroker.stop();
		zooKeeper.stop();
		Optional.of(JMPath.getPath("zookeeper-dir")).filter(JMPath::exists)
				.ifPresent(JMPathOperation::deleteDir);
		Optional.of(JMPath.getPath("kafka-broker-log")).filter(JMPath::exists)
				.ifPresent(JMPathOperation::deleteDir);
	}

	/**
	 * Test JM kafka streams.
	 *
	 * @throws Exception
	 *             the exception
	 */
	@Test
	public void testJMKafkaStreams() throws Exception {
		JMKStreamBuilder jmkStreamBuilder = new JMKStreamBuilder();
		Map<Integer, String> streamResultMap = new HashMap<>();
		jmkStreamBuilder.stream(new TypeReference<Map<Integer, String>>() {
		}, topic).foreach((key, value) -> streamResultMap.putAll(value));
		this.jmKafkaStreams = new JMKafkaStreams(applicationId, bootstrapServer,
				zookeeperConnect, jmkStreamBuilder);
		jmKafkaStreams.start();
		JMThread.sleep(1000);
		Map<Integer, String> testMap = JMStream.numberRangeClosed(1, 500, 1)
				.boxed().collect(toMap(getSelf(), i -> "Stream-" + i));
		kafkaProducer.sendJsonStringSync(testMap);
		JMThread.sleep(1000);
		System.out.println(testMap);
		System.out.println(streamResultMap);
		assertEquals(testMap.toString(), streamResultMap.toString());
	}

}
