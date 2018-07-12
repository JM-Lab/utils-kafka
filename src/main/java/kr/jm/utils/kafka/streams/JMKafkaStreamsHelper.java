package kr.jm.utils.kafka.streams;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLambda;
import kr.jm.utils.helper.JMLog;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * The type Jm output streams helper.
 */
public class JMKafkaStreamsHelper {
    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(JMKafkaStreamsHelper.class);
    private static final Consumed<String, String>
            STRING_STRING_CONSUMED = Consumed
            .with(Serdes.String(), Serdes.String());
    private static ObjectMapper objectMapper = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature
                    .READ_UNKNOWN_ENUM_VALUES_AS_NULL);


    private static <T> Optional<T> buildJsonStringAsOpt(
            ObjectMapper objectMapper, String jsonString,
            TypeReference<T> typeReference) {
        try {
            if (objectMapper == null)
                objectMapper = JMKafkaStreamsHelper.objectMapper;
            return Optional.of(objectMapper
                    .readValue(jsonString.getBytes(), typeReference));
        } catch (Exception e) {
            return JMExceptionManager.handleExceptionAndReturnEmptyOptional(log,
                    e, "buildJsonStringAsOpt", jsonString);
        }
    }

    /**
     * Build k stream topology with opt topology.
     *
     * @param <T>                the type parameter
     * @param additionalTopology the additional topology
     * @param typeReference      the type reference
     * @param topics             the topics
     * @return the topology
     */
    public static <T> Topology buildKStreamTopologyWithOpt(
            Consumer<KStream<String, Optional<T>>> additionalTopology,
            TypeReference<T> typeReference, String... topics) {
        return buildKStreamTopologyWithOpt(additionalTopology, null,
                typeReference,
                topics);
    }

    /**
     * Build k stream topology with opt topology.
     *
     * @param <T>                the type parameter
     * @param additionalTopology the additional topology
     * @param objectMapper       the object mapper
     * @param typeReference      the type reference
     * @param topics             the topics
     * @return the topology
     */
    public static <T> Topology buildKStreamTopologyWithOpt(
            Consumer<KStream<String, Optional<T>>> additionalTopology,
            ObjectMapper objectMapper, TypeReference<T> typeReference,
            String... topics) {
        return buildTopology(streamsBuilder -> additionalTopology
                .accept(buildKStreamWithOpt(streamsBuilder, objectMapper,
                        typeReference, topics)));
    }

    /**
     * Build k stream with opt k stream.
     *
     * @param <T>            the type parameter
     * @param streamsBuilder the streams builder
     * @param typeReference  the type reference
     * @param topics         the topics
     * @return the k stream
     */
    public static <T> KStream<String, Optional<T>> buildKStreamWithOpt(
            StreamsBuilder streamsBuilder, TypeReference<T> typeReference,
            String... topics) {
        return buildKStreamWithOpt(streamsBuilder, null, typeReference, topics);
    }

    /**
     * Build k stream with opt k stream.
     *
     * @param <T>            the type parameter
     * @param streamsBuilder the streams builder
     * @param objectMapper   the object mapper
     * @param typeReference  the type reference
     * @param topics         the topics
     * @return the k stream
     */
    public static <T> KStream<String, Optional<T>> buildKStreamWithOpt(
            StreamsBuilder streamsBuilder, ObjectMapper objectMapper,
            TypeReference<T> typeReference, String... topics) {
        return buildKStream(streamsBuilder, topics).mapValues(
                value -> buildJsonStringAsOpt(objectMapper, value,
                        typeReference));
    }

    /**
     * Build k stream k stream.
     *
     * @param streamsBuilder the streams builder
     * @param topics         the topics
     * @return the k stream
     */
    public static KStream<String, String> buildKStream(
            StreamsBuilder streamsBuilder, String[] topics) {
        return streamsBuilder
                .stream(Arrays.asList(topics), STRING_STRING_CONSUMED);
    }

    /**
     * Build k stream topology topology.
     *
     * @param <T>                the type parameter
     * @param additionalTopology the additional topology
     * @param typeReference      the type reference
     * @param topics             the topics
     * @return the topology
     */
    public static <T> Topology buildKStreamTopology(
            Consumer<KStream<String, T>> additionalTopology,
            TypeReference<T> typeReference, String... topics) {
        return buildKStreamTopology(additionalTopology, null, typeReference,
                topics);
    }

    /**
     * Build k stream topology topology.
     *
     * @param <T>                the type parameter
     * @param additionalTopology the additional topology
     * @param objectMapper       the object mapper
     * @param typeReference      the type reference
     * @param topics             the topics
     * @return the topology
     */
    public static <T> Topology buildKStreamTopology(
            Consumer<KStream<String, T>> additionalTopology,
            ObjectMapper objectMapper, TypeReference<T> typeReference,
            String... topics) {
        return buildTopology(streamsBuilder -> additionalTopology
                .accept(buildKStreamWithOpt(streamsBuilder, objectMapper,
                        typeReference, topics)
                        .filter(JMKafkaStreamsHelper::logAndFilter)
                        .mapValues(Optional::get)));
    }

    /**
     * Build topology topology.
     *
     * @param <T>                    the type parameter
     * @param streamsBuilderConsumer the streams builder consumer
     * @return the topology
     */
    public static <T> Topology buildTopology(
            Consumer<StreamsBuilder> streamsBuilderConsumer) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilderConsumer.accept(streamsBuilder);
        return streamsBuilder.build();
    }

    private static <T> boolean logAndFilter(String key, Optional<T> opt) {
        return JMLambda.consumeAndGetSelf(opt.isPresent(), b -> JMLambda
                .runIfTrue(b, () -> JMLog.warn(log, "logAndFilter", key, opt)));
    }

    /**
     * Build table topology with opt topology.
     *
     * @param <T>                the type parameter
     * @param additionalTopology the additional topology
     * @param typeReference      the type reference
     * @param topic              the topic
     * @return the topology
     */
    public static <T> Topology buildTableTopologyWithOpt(
            Consumer<KTable<String, Optional<T>>> additionalTopology,
            TypeReference<T> typeReference, String topic) {
        return buildTableTopologyWithOpt(additionalTopology, null,
                typeReference, topic);
    }

    /**
     * Build table topology with opt topology.
     *
     * @param <T>                the type parameter
     * @param additionalTopology the additional topology
     * @param objectMapper       the object mapper
     * @param typeReference      the type reference
     * @param topic              the topic
     * @return the topology
     */
    public static <T> Topology buildTableTopologyWithOpt(
            Consumer<KTable<String, Optional<T>>> additionalTopology,
            ObjectMapper objectMapper,
            TypeReference<T> typeReference, String topic) {
        return buildTopology(streamsBuilder -> additionalTopology.accept
                (buildKTableWithOpt(streamsBuilder, objectMapper, typeReference,
                        topic)));
    }

    /**
     * Build k table with opt k table.
     *
     * @param <T>            the type parameter
     * @param streamsBuilder the streams builder
     * @param typeReference  the type reference
     * @param topic          the topic
     * @return the k table
     */
    public static <T> KTable<String, Optional<T>> buildKTableWithOpt(
            StreamsBuilder streamsBuilder, TypeReference<T> typeReference,
            String topic) {
        return buildKTableWithOpt(streamsBuilder, null, typeReference, topic);
    }

    /**
     * Build k table with opt k table.
     *
     * @param <T>            the type parameter
     * @param streamsBuilder the streams builder
     * @param objectMapper   the object mapper
     * @param typeReference  the type reference
     * @param topic          the topic
     * @return the k table
     */
    public static <T> KTable<String, Optional<T>> buildKTableWithOpt(
            StreamsBuilder streamsBuilder, ObjectMapper objectMapper,
            TypeReference<T> typeReference, String topic) {
        return streamsBuilder.table(topic, STRING_STRING_CONSUMED)
                .mapValues(value -> buildJsonStringAsOpt(objectMapper, value,
                        typeReference));
    }

    /**
     * Build table topology topology.
     *
     * @param <T>                the type parameter
     * @param additionalTopology the additional topology
     * @param typeReference      the type reference
     * @param topic              the topic
     * @return the topology
     */
    public static <T> Topology buildTableTopology(
            Consumer<KTable<String, T>> additionalTopology,
            TypeReference<T> typeReference, String topic) {
        return buildTableTopology(additionalTopology, null, typeReference,
                topic);
    }

    /**
     * Build table topology topology.
     *
     * @param <T>                the type parameter
     * @param additionalTopology the additional topology
     * @param objectMapper       the object mapper
     * @param typeReference      the type reference
     * @param topic              the topic
     * @return the topology
     */
    public static <T> Topology buildTableTopology(
            Consumer<KTable<String, T>> additionalTopology,
            ObjectMapper objectMapper,
            TypeReference<T> typeReference, String topic) {
        return buildTopology(streamsBuilder -> additionalTopology
                .accept(buildKTableWithOpt(streamsBuilder, objectMapper,
                        typeReference, topic)
                        .filter(JMKafkaStreamsHelper::logAndFilter)
                        .mapValues(Optional::get)));
    }

    /**
     * Build output streams output streams.
     *
     * @param isLatest         the is latest
     * @param bootstrapServers the bootstrap servers
     * @param applicationId    the application id
     * @param topology         the topology
     * @return the output streams
     */
    public static KafkaStreams buildKafkaStreams(boolean isLatest,
            String bootstrapServers, String applicationId, Topology topology) {
        return buildKafkaStreams(topology, new Properties() {
            {
                put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        isLatest ? "latest" : "earliest");
                put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                        Serdes.String().getClass());
                put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                        Serdes.String().getClass());
            }
        });
    }

    /**
     * Build output streams output streams.
     *
     * @param topology               the topology
     * @param kafkaStreamsProperties the output streams properties
     * @return the output streams
     */
    public static KafkaStreams buildKafkaStreams(Topology topology,
            Properties kafkaStreamsProperties) {
        JMLog.info(log, "buildKafkaStreams", topology.describe(),
                kafkaStreamsProperties);
        KafkaStreams kafkaStreams =
                new KafkaStreams(topology, kafkaStreamsProperties);
        kafkaStreams.setUncaughtExceptionHandler((t, e) -> JMExceptionManager
                .logException(log, e, "uncaughtException", t));
        return kafkaStreams;
    }

    /**
     * Build output streams with start output streams.
     *
     * @param bootstrapServers the bootstrap servers
     * @param applicationId    the application id
     * @param topology         the topology
     * @return the output streams
     */
    public static KafkaStreams buildKafkaStreamsWithStart(
            String bootstrapServers, String applicationId, Topology topology) {
        return startKafkaStream(
                buildKafkaStreams(false, bootstrapServers, applicationId,
                        topology));
    }

    /**
     * Build output streams with start output streams.
     *
     * @param KafkaStreamsProperties the output streams properties
     * @param topology               the topology
     * @return the output streams
     */
    public static KafkaStreams buildKafkaStreamsWithStart(
            Properties KafkaStreamsProperties, Topology topology) {
        return startKafkaStream(
                buildKafkaStreams(topology, KafkaStreamsProperties));
    }

    private static KafkaStreams startKafkaStream(KafkaStreams kafkaStreams) {
        JMLog.info(log, "startKafkaStream");
        kafkaStreams.start();
        return kafkaStreams;
    }
}
