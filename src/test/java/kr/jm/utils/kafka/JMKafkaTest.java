package kr.jm.utils.kafka;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import kr.jm.utils.AutoStringBuilder;
import kr.jm.utils.enums.OS;
import kr.jm.utils.helper.JMPath;
import kr.jm.utils.helper.JMPathOperation;
import kr.jm.utils.helper.JMStream;
import kr.jm.utils.helper.JMString;
import kr.jm.utils.helper.JMThread;
import kr.jm.utils.zookeeper.JMZookeeperServer;

public class JMKafkaTest {

	private String groupId = "test";
	private String topic = "testLocal";
	private String lastValue = "lastValue";
	private JMZookeeperServer zooKeeper;
	private JMKafkaBroker kafkaBroker;
	private JMKafkaProducer kafkaProducer;
	private JMKafkaConsumer kafkaConsumer;
	private String bootstrapServer;

	@Before
	public void setUp() throws Exception {
		this.zooKeeper = new JMZookeeperServer();
		this.zooKeeper.start();
		JMThread.sleep(5000);
		this.kafkaBroker = new JMKafkaBroker(JMString.buildIpOrHostnamePortPair(
				OS.getHostname(), zooKeeper.getClientPort()));
		this.kafkaBroker.startup();
		JMThread.sleep(5000);
		bootstrapServer = JMString.buildIpOrHostnamePortPair(OS.getHostname(),
				kafkaBroker.getPort());
		this.kafkaProducer = new JMKafkaProducer(bootstrapServer, topic);
		List<Integer> numList =
				JMStream.numberRangeClosed(1, 500, 1).boxed().collect(toList());
		kafkaProducer.sendJsonStringList("number", numList);
		kafkaProducer.sendSync(topic, lastValue);
		OS.addShutdownHook(() -> JMPathOperation
				.deleteDir(JMPath.getPath("zookeeper-dir")));
		OS.addShutdownHook(() -> JMPathOperation
				.deleteDir(JMPath.getPath("kafka-broker-log")));
	}

	@After
	public void tearDown() {
		kafkaProducer.close();
		kafkaConsumer.close();
		kafkaBroker.stop();
		zooKeeper.stop();
	}

	@Test
	public final void testStart() throws Exception {
		LongAdder indexAdder = new LongAdder();
		AutoStringBuilder resultString = new AutoStringBuilder(",");
		this.kafkaConsumer = new JMKafkaConsumer(bootstrapServer, false,
				groupId, consumerRecords -> {
					indexAdder.add(consumerRecords.count());
					consumerRecords
							.forEach(cr -> resultString.append(cr.value()));
				}, topic);
		this.kafkaConsumer.run();
		JMThread.sleep(2000);
		assertEquals(501, indexAdder.intValue());
		String[] split = resultString.autoToString().split(",");
		assertTrue(lastValue.equals(split[split.length - 1]));
	}
}
