/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.chribro88.kafka.connect.transforms.partitions;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.transforms.SmtManager;
import io.debezium.util.MurmurHash3;

/**
 * This SMT allow to use payload fields to calculate the destination partition.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Mario Fiore Vitale
 */
public class HeaderPartitionRouting<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeaderPartitionRouting.class);
    private static final MurmurHash3 MURMUR_HASH_3 = MurmurHash3.getInstance();
    public static final String NESTING_SEPARATOR = "\\.";
    public static final String CHANGE_SPECIAL_FIELD = "change";
    public static final String FIELD_HEADER_FIELD_CONF = "partition.header.fields";
    public static final String FIELD_TOPIC_PARTITION_NUM_CONF = "partition.topic.num";
    public static final String FIELD_HASH_FUNCTION = "partition.hash.function";

    public enum HashFunction implements EnumeratedValue {
        /**
         * Hash function to be used when computing hash of the fields of the record.
         */

        JAVA("java", Object::hashCode),
        MURMUR("murmur", MURMUR_HASH_3::hash),
        PASSTHROUGH("passthrough", value -> (int) (Long.parseLong((String) value) & Integer.MAX_VALUE) );

        private final String name;
        private final Function<Object, Integer> hash;

        HashFunction(String value, Function<Object, Integer> hash) {
            this.name = value;
            this.hash = hash;
        }

        @Override
        public String getValue() {
            return name;
        }

        public Function<Object, Integer> getHash() {
            return hash;
        }

        public static HashFunction parse(String value) {
            if (value == null) {    
                return JAVA;
            }
            value = value.trim().toLowerCase();
            for (HashFunction option : HashFunction.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return JAVA;
        }
    }

    static final Field PARTITION_HEADER_FIELDS_FIELD = Field.create(FIELD_HEADER_FIELD_CONF)
            .withDisplayName("List of header fields to use for compute partition.")
            .withType(ConfigDef.Type.LIST)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(
                    Field::notContainEmptyElements)
            .withDescription("Header fields to use to calculate the partition.")
            .required();

    static final Field TOPIC_PARTITION_NUM_FIELD = Field.create(FIELD_TOPIC_PARTITION_NUM_CONF)
            .withDisplayName("Number of partition configured for topic")
            .withType(ConfigDef.Type.INT)
            .withValidation(Field::isPositiveInteger)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Number of partition for the topic on which this SMT act. Use TopicNameMatches predicate to filter records by topic")
            .required();

    static final Field HASH_FUNCTION_FIELD = Field.create(FIELD_HASH_FUNCTION)
            .withDisplayName("Hash function")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Hash function to be used when computing hash of the fields which would determine number of the destination partition.")
            .withDefault("java")
            .optional();

    private SmtManager<R> smtManager;
    private List<String> headerFields;
    private int partitionNumber;
    private HashFunction hashFc;

    @Override
    public ConfigDef config() {

        ConfigDef config = new ConfigDef();
        // group does not manage validator definition. Validation will not work here.
        return Field.group(config, "partitions",
                PARTITION_HEADER_FIELDS_FIELD, TOPIC_PARTITION_NUM_FIELD, HASH_FUNCTION_FIELD);
    }

    @Override
    public void configure(Map<String, ?> props) {

        final Configuration config = Configuration.from(props);

        smtManager = new SmtManager<>(config);

        smtManager.validate(config, Field.setOf(PARTITION_HEADER_FIELDS_FIELD, TOPIC_PARTITION_NUM_FIELD));

        headerFields = config.getList(PARTITION_HEADER_FIELDS_FIELD);
        partitionNumber = config.getInteger(TOPIC_PARTITION_NUM_FIELD);
        hashFc = HashFunction.parse(config.getString(HASH_FUNCTION_FIELD));
    }

    @Override
    public R apply(R originalRecord) {

        LOGGER.trace("Starting HeaderPartitionRoutingB SMT with conf: {} {}", headerFields, partitionNumber);

        if (originalRecord.headers().isEmpty()) {
            LOGGER.trace("No headers, passing it unchanged ");
            return originalRecord;
        }
        
        try {

            // byte[] headerValue = getHeaderValue(originalRecord.headers(), headerFields);
            List<Object> headersValue = headerFields.stream()
                    .map(headerKey -> getHeaderValue(originalRecord.headers(), headerKey))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());

            if (headersValue.isEmpty()) {
                LOGGER.trace("None of the configured headers found on record {}. Skipping it.", originalRecord.headers().toString());
                return originalRecord;
            }

            // int partition = computePartition(partitionNumber, fieldsValue);
            int partition = computePartition(partitionNumber, headersValue);

            return buildNewRecord(originalRecord, partition);

        }
        catch (Exception e) {
            throw new DebeziumException(String.format("Unprocessable message %s", originalRecord.headers().toString()), e);
        }
    }

    private Optional<String> getHeaderValue(Headers headers, String headerKey) {
        Header header = headers.lastWithName(headerKey);
        if (header != null) {
            if (header.value() instanceof byte[]) {
                byte[] bytes = (byte[]) header.value();
                String stringValue = new String(bytes, StandardCharsets.UTF_8);
                return Optional.ofNullable(stringValue);
            } else if (header.value() instanceof String) {
                return Optional.ofNullable((String) header.value());
            } else {
                LOGGER.warn("Unexpected type for header value of key {}: {}", headerKey, header.value().getClass());
                return Optional.empty();
            }
        } else {
            LOGGER.trace("Field {} not found on headers {}. It will not be considered", headerKey, headers);
            return Optional.empty();
        }
    }
    


    private R buildNewRecord(R originalRecord, int partition) {
        LOGGER.trace("Record will be sent to partition {}", partition);

        return originalRecord.newRecord(originalRecord.topic(), partition,
                originalRecord.keySchema(),
                originalRecord.key(),
                originalRecord.valueSchema(),
                originalRecord.value(),
                originalRecord.timestamp(),
                originalRecord.headers());
    }


    protected int computePartition(Integer partitionNumber, List<Object> values) {
        int totalHashCode = values.stream().map(hashFc.getHash()).reduce(0, Integer::sum);
        // hashCode can be negative due to overflow. Since Math.abs(Integer.MIN_INT) will still return a negative number
        // we use bitwise operation to remove the sign
        int normalizedHash = totalHashCode & Integer.MAX_VALUE;
        if (normalizedHash == Integer.MAX_VALUE) {
            normalizedHash = 0;
        }
        return normalizedHash % partitionNumber;
    }


    @Override
    public void close() {
    }
}
