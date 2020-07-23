package kr.jm.utils.kafka;

import kafka.admin.RackAwareMode;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;
import kr.jm.utils.helper.JMThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import scala.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The type Jm kafka admin.
 */
public class JMKafkaAdmin {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(JMKafkaAdmin.class);
    private String zookeeperConnect;
    private Properties topicConsumerProperties;
    private int sessionTimeoutMs = 3 * 1000;
    private int connectionTimeoutMs = 3 * 1000;
    private boolean isSecureKafkaCluster = false;

    /**
     * Instantiates a new Jm kafka admin.
     *
     * @param zookeeperConnect the zookeeper connect
     * @param bootstrapServers the bootstrap servers
     */
    public JMKafkaAdmin(String zookeeperConnect, String bootstrapServers) {
        this.zookeeperConnect = zookeeperConnect;
        this.topicConsumerProperties = new Properties();
        this.topicConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.topicConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "jmKafkaAdmin");
        String deserializer = Serdes.String().deserializer().getClass().getName();
        this.topicConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        this.topicConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
    }

    private KafkaZkClient getKafkaZkClient() {
        return KafkaZkClient
                .apply(zookeeperConnect, isSecureKafkaCluster, sessionTimeoutMs, connectionTimeoutMs, 10, Time.SYSTEM,
                        "jmKafkaGroup", "jmKafkaType", Option.empty());
    }

    private <R> R operationFunction(Function<AdminZkClient, R> operationFunction, String methodName, Object... params) {
        JMLog.info(log, methodName, params);
        KafkaZkClient kafkaZkClient = null;
        try {
            kafkaZkClient = getKafkaZkClient();
            return operationFunction.apply(new AdminZkClient(kafkaZkClient));
        } catch (Exception e) {
            return JMExceptionManager.handleExceptionAndReturnNull(log, e, methodName, params);
        } finally {
            if (kafkaZkClient != null)
                kafkaZkClient.close();
        }
    }

    private void operation(Consumer<AdminZkClient> operationConsumer, String methodName, Object... params) {
        JMLog.info(log, methodName, params);
        KafkaZkClient kafkaZkClient = null;
        try {
            operationConsumer.accept(new AdminZkClient(kafkaZkClient = getKafkaZkClient()));
            JMThread.sleep(1000);
        } catch (Exception e) {
            JMExceptionManager.handleException(log, e, methodName, params);
        } finally {
            if (kafkaZkClient != null)
                kafkaZkClient.close();
        }
    }

    /**
     * Create topic.
     *
     * @param topic           the topic
     * @param partitions      the partitions
     * @param replication     the replication
     * @param topicProperties the topic properties
     */
    public void createTopic(String topic, int partitions, int replication,
            Properties topicProperties) {
        operation(adminZkClient -> adminZkClient
                        .createTopic(topic, partitions, replication, topicProperties,
                                RackAwareMode.Enforced$.MODULE$),
                "createTopic", topic, partitions, replication, topicProperties);
    }

    /**
     * Create topic.
     *
     * @param topic       the topic
     * @param partitions  the partitions
     * @param replication the replication
     */
    public void createTopic(String topic, int partitions, int replication) {
        createTopic(topic, partitions, replication, new Properties());
    }

    /**
     * Delete topic.
     *
     * @param topic the topic
     */
    public void deleteTopic(String topic) {
        // if delete.topic.enable=true
        operation(adminZkClient -> adminZkClient.deleteTopic(topic),
                "deleteTopic", topic);
    }

    /**
     * Topic exists boolean.
     *
     * @param topic the topic
     * @return the boolean
     */
    public boolean topicExists(String topic) {
        return getKafkaZkClient().topicExists(topic);
    }

    /**
     * Gets all topic info.
     *
     * @return the all topic info
     */
    public Map<String, List<PartitionInfo>> getAllTopicInfo() {
        return getTopicConsumer().listTopics();
    }

    private KafkaConsumer<String, String> getTopicConsumer() {
        return new KafkaConsumer<>(topicConsumerProperties);
    }

    /**
     * Gets topic list.
     *
     * @return the topic list
     */
    public List<String> getTopicList() {
        return new ArrayList<>(getAllTopicInfo().keySet());
    }

    /**
     * Gets partition info.
     *
     * @param topic the topic
     * @return the partition info
     */
    public List<PartitionInfo> getPartitionInfo(String topic) {
        return getAllTopicInfo().get(topic);
    }

    /**
     * Gets partition count.
     *
     * @param topic the topic
     * @return the partition count
     */
    public int getPartitionCount(String topic) {
        return getPartitionInfo(topic).size();
    }

    /**
     * Gets zookeeper connect.
     *
     * @return the zookeeper connect
     */
    public String getZookeeperConnect() {
        return zookeeperConnect;
    }

    /**
     * Gets session timeout ms.
     *
     * @return the session timeout ms
     */
    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    /**
     * Sets session timeout ms.
     *
     * @param sessionTimeoutMs the session timeout ms
     */
    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    /**
     * Gets connection timeout ms.
     *
     * @return the connection timeout ms
     */
    public int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    /**
     * Sets connection timeout ms.
     *
     * @param connectionTimeoutMs the connection timeout ms
     */
    public void setConnectionTimeoutMs(int connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    /**
     * Is secure kafka cluster boolean.
     *
     * @return the boolean
     */
    public boolean isSecureKafkaCluster() {
        return isSecureKafkaCluster;
    }

    /**
     * Sets secure kafka cluster.
     *
     * @param isSecureKafkaCluster the is secure kafka cluster
     */
    public void setSecureKafkaCluster(boolean isSecureKafkaCluster) {
        this.isSecureKafkaCluster = isSecureKafkaCluster;
    }

}
