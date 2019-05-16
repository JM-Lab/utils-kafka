JMLab Utility Libraries For Kafka 1.1.x
========================================

## Useful Functions :
* **Embedded Kafka For Test - JMKafkaServer**
* **Embedded Zookeeper For Test - JMZookeeperServer**
* **Kafka Consumer With JSON String - JMKafkaConsumer**
* **Kafka Producer With JSON String - JMKafkaProducer**
* **Kafka Streams Utility - JMKafkaStreamsHelper**

## version
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.jm-lab/jmlab-utils-kafka/badge.svg)](http://search.maven.org/#artifactdetails%7Ckr.jmlab%7Cjmlab-utils-kafka%7C2.1.1%7Cjar)

## Prerequisites:
* Java 8 or later

## Installation

Checkout the source code:

    git clone https://github.com/JM-Lab/utils-kafka.git
    cd utils-kafka
    git checkout -b 2.1.1 origin/2.1.1 
    mvn install

## Usage
Set up pom.xml :

    (...)
    <dependency>
			<groupId>kr.jmlab</groupId>
			<artifactId>jmlab-utils-kafka</artifactId>
			<version>2.1.1</version>
	</dependency>
    (...)

For example 1 ([JMKafkaClientTest.java](https://github.com/JM-Lab/utils-kafka/blob/master/src/test/java/kr/jm/utils/kafka/client/JMKafkaClientTest.java))

```java
this.embeddedZookeeper = new JMZookeeperServer(OS.getAvailableLocalPort()).start();
String zookeeperConnect = this.embeddedZookeeper.getZookeeperConnect();
this.kafkaServer = new JMKafkaServer(zookeeperConnect, OS.getAvailableLocalPort()).start();
sleep(3000);
this.bootstrapServer = kafkaServer.getKafkaServerConnect();
this.kafkaProducer = new JMKafkaProducer(bootstrapServer).withDefaultTopic(topic);
kafkaProducer.send("key", lastValue);
List<Integer> numList = JMStream.numberRangeClosed(1, 500, 1).boxed().collect(toList());
kafkaProducer.sendJsonStringList("number", numList);
```
For example 2 ([JMKafkaStreamsTest.java](https://github.com/JM-Lab/utils-kafka/blob/master/src/test/java/kr/jm/utils/kafka/streams/JMKafkaStreamsTest.java))

```java
Topology topology = JMKafkaStreamsHelper.buildKStreamTopology(
    stringKStream -> stringKStream.foreach((key, value) -> streamResultMap.putAll(value)),
    new TypeReference<Map<Integer, String>>() {}, topic);
this.kafkaStreams = JMKafkaStreamsHelper.buildKafkaStreamsWithStart(bootstrapServer, applicationId, topology);
```