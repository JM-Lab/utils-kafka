package kr.jm.utils.kafka.streams;

import java.util.Optional;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import kr.jm.utils.exception.JMExceptionManager;

/**
 * The Class JMKStreamBuilder.
 */
public class JMKStreamBuilder extends KStreamBuilder {
	private static final org.slf4j.Logger log =
			org.slf4j.LoggerFactory.getLogger(JMKStreamBuilder.class);
	private static final Serde<String> SerdeString = Serdes.String();
	private ObjectMapper objectMapper = new ObjectMapper()
			.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
			.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL);

	private <T> Optional<T> buildJsonStringAsOpt(String jsonString,
			TypeReference<T> typeReference) {
		try {
			return Optional.of(objectMapper.readValue(jsonString.getBytes(),
					typeReference));
		} catch (Exception e) {
			return JMExceptionManager.handleExceptionAndReturnEmptyOptional(log,
					e, "buildJsonStringAsOpt", jsonString);
		}
	}

	/**
	 * Stream with opt.
	 *
	 * @param <T>
	 *            the generic type
	 * @param typeReference
	 *            the type reference
	 * @param topics
	 *            the topics
	 * @return the k stream
	 */
	public <T> KStream<String, Optional<T>>
			streamWithOpt(TypeReference<T> typeReference, String... topics) {
		return stream(SerdeString, SerdeString, topics)
				.mapValues(value -> buildJsonStringAsOpt(value, typeReference));
	}

	/**
	 * Stream.
	 *
	 * @param <T>
	 *            the generic type
	 * @param typeReference
	 *            the type reference
	 * @param topics
	 *            the topics
	 * @return the k stream
	 */
	public <T> KStream<String, T> stream(TypeReference<T> typeReference,
			String... topics) {
		return streamWithOpt(typeReference, topics)
				.filter((key, opt) -> opt.isPresent())
				.mapValues(opt -> opt.get());
	}

	/**
	 * Table with opt.
	 *
	 * @param <T>
	 *            the generic type
	 * @param typeReference
	 *            the type reference
	 * @param topic
	 *            the topic
	 * @param storeName
	 *            the store name
	 * @return the k table
	 */
	public <T> KTable<String, Optional<T>> tableWithOpt(
			TypeReference<T> typeReference, String topic, String storeName) {
		return table(SerdeString, SerdeString, topic, storeName)
				.mapValues(value -> buildJsonStringAsOpt(value, typeReference));
	}

	/**
	 * Table.
	 *
	 * @param <T>
	 *            the generic type
	 * @param typeReference
	 *            the type reference
	 * @param topic
	 *            the topic
	 * @param storeName
	 *            the store name
	 * @return the k table
	 */
	public <T> KTable<String, T> table(TypeReference<T> typeReference,
			String topic, String storeName) {
		return tableWithOpt(typeReference, topic, storeName)
				.filter((key, opt) -> opt.isPresent())
				.mapValues(opt -> opt.get());
	}

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

}
