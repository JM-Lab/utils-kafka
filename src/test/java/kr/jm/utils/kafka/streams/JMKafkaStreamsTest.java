package kr.jm.utils.kafka.streams;

import com.fasterxml.jackson.core.type.TypeReference;
import kr.jm.utils.enums.OS;
import kr.jm.utils.helper.JMPath;
import kr.jm.utils.helper.JMPathOperation;
import kr.jm.utils.helper.JMStream;
import kr.jm.utils.helper.JMThread;
import kr.jm.utils.kafka.JMKafkaServer;
import kr.jm.utils.kafka.client.JMKafkaProducer;
import kr.jm.utils.zookeeper.JMZookeeperServer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.*;
import static kr.jm.utils.helper.JMLambda.getSelf;
import static org.junit.Assert.assertEquals;

/**
 * The type Jm kafka streams test.
 */
public class JMKafkaStreamsTest {
    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    }

    private String topic = "testStreamLocal";
    private JMZookeeperServer zooKeeper;
    private JMKafkaServer kafkaServer;
    private JMKafkaProducer kafkaProducer;
    private String bootstrapServer;

    private String applicationId = "testKafkaStream";
    private KafkaStreams kafkaStreams;

    /**
     * Instantiates a new Jm kafka streams test.
     */
    public JMKafkaStreamsTest() {
        Optional.of(JMPath.getPath(JMZookeeperServer.DEFAULT_ZOOKEEPER_DIR))
                .filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
        Optional.of(JMPath.getPath(JMKafkaServer.DEFAULT_KAFKA_LOG))
                .filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
        JMThread.sleep(1000);
    }

    /**
     * Sets up.
     */
    @Before
    public void setUp() {
        Optional.of(JMPath.getPath(JMZookeeperServer.DEFAULT_ZOOKEEPER_DIR))
                .filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
        Optional.of(JMPath.getPath(JMKafkaServer.DEFAULT_KAFKA_LOG))
                .filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
        this.zooKeeper =
                new JMZookeeperServer(OS.getAvailableLocalPort()).start();
        this.kafkaServer =
                new JMKafkaServer.Builder(zooKeeper.getZookeeperConnect())
                        .build().start();
        JMThread.sleep(3000);
        this.bootstrapServer = kafkaServer.getKafkaServerConnect();
        this.kafkaProducer = new JMKafkaProducer(bootstrapServer)
                .withDefaultTopic(topic);
    }

    /**
     * Tear down.
     */
    @After
    public void tearDown() {
        kafkaStreams.close();
        kafkaProducer.close();
        kafkaServer.stop();
        zooKeeper.stop();
        Optional.of(JMPath.getPath(JMZookeeperServer.DEFAULT_ZOOKEEPER_DIR))
                .filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
        Optional.of(JMPath.getPath(JMKafkaServer.DEFAULT_KAFKA_LOG))
                .filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
    }

    /**
     * Test jm kafka streams.
     */
    @Test
    public void testJMKafkaStreams() {
        Map<Integer, String> testMap = JMStream.numberRangeClosed(1, 500, 1)
                .boxed().collect(toMap(getSelf(), i -> "Stream-" + i));
        kafkaProducer.sendJsonStringSync(testMap);
        Map<Integer, String> streamResultMap = new HashMap<>();
        Topology topology = JMKafkaStreamsHelper.buildKStreamTopology(
                stringKStream -> stringKStream.foreach(
                        (key, value) -> streamResultMap.putAll(value)),
                new TypeReference<Map<Integer, String>>() {
                }, topic);
        this.kafkaStreams =
                JMKafkaStreamsHelper.buildKafkaStreamsWithStart(bootstrapServer,
                        applicationId, topology);
        JMThread.sleep(5000);

        System.out.println(testMap);
        System.out.println(streamResultMap);
        assertEquals(testMap.toString(), streamResultMap.toString());
    }

}
