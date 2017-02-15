JMLab Utility Libraries For Kafka 0.10.x
========================================

## Useful Functions :
* **Embeded Kafka Broker For Test - JMKafkaBroker**
* **Embeded Zookeeper For Test - JMZookeeperServer**
* **Kafka Consumer Server - JMKafkaConsumer**
* **Kafka Producer To Support JSON String - JMKafkaProducer**
* **Kafka Streams Utility - JMKafkaStreams**
* ***JMKStreamBuilder***

## version
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.jm-lab/jmlab-utils-kafka/badge.svg)](http://search.maven.org/#artifactdetails%7Ccom.github.jm-lab%7Cjmlab-utils-kafka%7C0.10.1%7Cjar)

## Prerequisites:
* Java 8 or later

## Installation

Checkout the source code:

    git clone https://github.com/JM-Lab/utils-kafka.git
    cd utils-kafka
    git checkout -b 0.10.1 origin/0.10.1 
    mvn install

## Usage
Set up pom.xml :

    (...)
    <dependency>
			<groupId>com.github.jm-lab</groupId>
			<artifactId>jmlab-utils-kafka</artifactId>
			<version>0.10.1</version>
	</dependency>
    (...)

For example ([JMKafkaClientTest.java](https://github.com/JM-Lab/utils-kafka/blob/master/src/test/java/kr/jm/utils/kafka/client/JMKafkaClientTest.java))

```java
this.zooKeeper = new JMZookeeperServer();
this.zooKeeper.start();
JMThread.sleep(5000);
this.kafkaBroker = new JMKafkaBroker(JMString.buildIpOrHostnamePortPair(OS.getHostname(), zooKeeper.getClientPort()));
this.kafkaBroker.startup();
JMThread.sleep(5000);
bootstrapServer = JMString.buildIpOrHostnamePortPair(OS.getHostname(), kafkaBroker.getPort());
this.kafkaProducer = new JMKafkaProducer(bootstrapServer, topic);
List<Integer> numList =	JMStream.numberRangeClosed(1, 500, 1).boxed().collect(toList());
kafkaProducer.sendJsonStringList("number", numList);
kafkaProducer.sendSync(topic, lastValue);
```