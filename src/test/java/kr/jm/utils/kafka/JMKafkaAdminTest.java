package kr.jm.utils.kafka;

import kr.jm.utils.enums.OS;
import kr.jm.utils.helper.JMPath;
import kr.jm.utils.helper.JMPathOperation;
import kr.jm.utils.helper.JMThread;
import kr.jm.utils.zookeeper.JMZookeeperServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * The type Jm kafka admin test.
 */
public class JMKafkaAdminTest {

    private JMKafkaAdmin jmKafkaAdmin;
    private JMZookeeperServer zooKeeper;
    private JMKafkaServer jmKafkaServer;

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
        this.zooKeeper = new JMZookeeperServer();
        this.zooKeeper.start();
        JMThread.sleep(3000);
        String zookeeperConnect = zooKeeper.getZookeeperConnect();
        Properties brokerProperties =
                new JMKafkaServer(zookeeperConnect, OS.getAvailableLocalPort())
                        .getKafkaServerProperties();
        brokerProperties.setProperty("delete.topic.enable", "true");
        this.jmKafkaServer = new JMKafkaServer(brokerProperties).start();
        JMThread.sleep(3000);
        this.jmKafkaAdmin = new JMKafkaAdmin(zookeeperConnect,
                jmKafkaServer.getKafkaServerConnect());
    }

    /**
     * Tear down.
     */
    @After
    public void tearDown() {
        jmKafkaServer.stop();
        zooKeeper.stop();
        Optional.of(JMPath.getPath(JMZookeeperServer.DEFAULT_ZOOKEEPER_DIR))
                .filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
        Optional.of(JMPath.getPath(JMKafkaServer.DEFAULT_KAFKA_LOG))
                .filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
    }

    /**
     * Test jm kafka admin.
     */
    @Test
    public void testJMKafkaAdmin() {
        System.out.println(jmKafkaAdmin.getAllTopicInfo());
        String topic = "newTestTopic";
        jmKafkaAdmin.createTopic(topic, 1, 1);
        System.out.println(jmKafkaAdmin.getAllTopicInfo());
        System.out.println(jmKafkaAdmin.getTopicList());
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
