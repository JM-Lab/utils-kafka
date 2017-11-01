package kr.jm.utils.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

import kafka.admin.RackAwareMode;
import kr.jm.utils.helper.JMThread;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;

public class JMKafkaAdmin {
    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(JMKafkaAdmin.class);
    private String zookeeperConnect;
    private Properties topicProperties;
    private int sessionTimeoutMs = 3 * 1000;
    private int connectionTimeoutMs = 3 * 1000;
    private boolean isSecureKafkaCluster = false;

    public JMKafkaAdmin(String zookeeperConnect, String bootstrapServers) {
        this.zookeeperConnect = zookeeperConnect;
        this.topicProperties = new Properties();
        this.topicProperties.put("bootstrap.servers", bootstrapServers);
        this.topicProperties.put("group.id", "jmKafkaAdmin");
        this.topicProperties.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        this.topicProperties.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
    }

    private ZkUtils getZkUtils() {
        return ZkUtils.apply(zookeeperConnect, sessionTimeoutMs,
                connectionTimeoutMs, isSecureKafkaCluster);
    }

    private <R> R operationFunction(Function<ZkUtils, R> operationFunction,
            String methodName, Object... params) {
        JMLog.info(log, methodName, params);
        ZkUtils zkUtils = null;
        try {
            zkUtils = getZkUtils();
            return operationFunction.apply(zkUtils);
        } catch (Exception e) {
            return JMExceptionManager.handleExceptionAndReturnNull(log, e,
                    methodName, params);
        } finally {
            if (zkUtils != null)
                zkUtils.close();
        }
    }

    private void operation(Consumer<ZkUtils> operationConsumer,
            String methodName, Object... params) {
        operationFunction(zkUtils -> {
            operationConsumer.accept(zkUtils);
            JMThread.sleep(1000);
            return null;
        }, methodName, params);
    }

    public void createTopic(String topic, int partitions, int replication,
            Properties topicProperties) {
        operation(
                zkUtils -> AdminUtils.createTopic(zkUtils, topic, partitions,
                        replication, topicProperties,
                        RackAwareMode.Enforced$.MODULE$),
                "createTopic", topic, partitions, replication, topicProperties);
    }

    public void createTopic(String topic, int partitions, int replication) {
        createTopic(topic, partitions, replication, new Properties());
    }

    public void deleteTopic(String topic) {
        // if delete.topic.enable=true
        operation(zkUtils -> AdminUtils.deleteTopic(zkUtils, topic),
                "deleteTopic", topic);
    }

    public boolean topicExists(String topic) {
        return operationFunction(
                zkUtils -> AdminUtils.topicExists(zkUtils, topic),
                "topicExists", topic);
    }

    public Map<String, List<PartitionInfo>> getAllTopicInfo() {
        return getTopicConsumer().listTopics();
    }

    private KafkaConsumer<String, String> getTopicConsumer() {
        return new KafkaConsumer<>(topicProperties);
    }

    public List<String> getTopicList() {
        return new ArrayList<>(getAllTopicInfo().keySet());
    }

    public List<PartitionInfo> getPartitionInfo(String topic) {
        return getAllTopicInfo().get(topic);
    }

    public int getPartitionCount(String topic) {
        return getPartitionInfo(topic).size();
    }

    public String getZookeeperConnect() {
        return zookeeperConnect;
    }

    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    public void setConnectionTimeoutMs(int connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    public boolean isSecureKafkaCluster() {
        return isSecureKafkaCluster;
    }

    public void setSecureKafkaCluster(boolean isSecureKafkaCluster) {
        this.isSecureKafkaCluster = isSecureKafkaCluster;
    }

}
