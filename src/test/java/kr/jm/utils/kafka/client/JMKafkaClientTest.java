package kr.jm.utils.kafka.client;

import kr.jm.utils.AutoStringBuilder;
import kr.jm.utils.enums.OS;
import kr.jm.utils.helper.JMPath;
import kr.jm.utils.helper.JMPathOperation;
import kr.jm.utils.helper.JMStream;
import kr.jm.utils.kafka.JMKafkaServer;
import kr.jm.utils.zookeeper.JMZookeeperServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

import static java.util.stream.Collectors.toList;
import static kr.jm.utils.helper.JMThread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The type Jm kafka client test.
 */
public class JMKafkaClientTest {

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    }

    private String groupId = "test";
    private String topic = "testLocal";
    private String lastValue = "lastValue";
    private JMZookeeperServer embeddedZookeeper;
    private JMKafkaServer kafkaServer;
    private JMKafkaProducer kafkaProducer;
    private JMKafkaConsumer kafkaConsumer;
    private String bootstrapServer;

    /**
     * Sets up.
     *
     */
    @Before
    public void setUp() {
        Optional.of(JMPath.getPath(JMZookeeperServer.DEFAULT_ZOOKEEPER_DIR))
                .filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
        Optional.of(JMPath.getPath(JMKafkaServer.DEFAULT_KAFKA_LOG))
                .filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
        this.embeddedZookeeper =
                new JMZookeeperServer(OS.getAvailableLocalPort()).start();
        String zookeeperConnect = this.embeddedZookeeper.getZookeeperConnect();
        this.kafkaServer =
                new JMKafkaServer(zookeeperConnect, OS.getAvailableLocalPort())
                        .start();
        sleep(3000);
        this.bootstrapServer = kafkaServer.getKafkaServerConnect();
        this.kafkaProducer = new JMKafkaProducer(bootstrapServer)
                .withDefaultTopic(topic);
        kafkaProducer.send("key", lastValue);
        List<Integer> numList =
                JMStream.numberRangeClosed(1, 500, 1).boxed().collect(toList());
        kafkaProducer.sendJsonStringList("number", numList);
    }

    /**
     * Tear down.
     */
    @After
    public void tearDown() {
        kafkaProducer.close();
        kafkaConsumer.shutdown();
        kafkaServer.stop();
        embeddedZookeeper.stop();
        Optional.of(JMPath.getPath(JMZookeeperServer.DEFAULT_ZOOKEEPER_DIR))
                .filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
        Optional.of(JMPath.getPath(JMKafkaServer.DEFAULT_KAFKA_LOG))
                .filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
    }

    /**
     * Test start.
     *
     */
    @Test
    public final void testStart() {
        LongAdder indexAdder = new LongAdder();
        AutoStringBuilder resultString = new AutoStringBuilder(",");
        this.kafkaConsumer =
                new JMKafkaConsumer(false, bootstrapServer, groupId,
                        consumerRecord -> {
                            indexAdder.increment();
                            resultString.append(consumerRecord.value());
                            System.out
                                    .printf("offset = %d, key = %s, value = %s%n",
                                            consumerRecord.offset(),
                                            consumerRecord.key(),
                                            consumerRecord.value());
                        }, topic).start();
        sleep(5000);
        assertEquals(501, indexAdder.intValue());
        String[] split = resultString.autoToString().split(",");
        assertTrue(lastValue.equals(split[0]));
    }


}
