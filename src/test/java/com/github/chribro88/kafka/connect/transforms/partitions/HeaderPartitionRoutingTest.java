/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.chribro88.kafka.connect.transforms.partitions;

import static io.debezium.data.Envelope.Operation.*;
import static org.assertj.core.api.Assertions.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import com.github.chribro88.kafka.connect.transforms.doc.FixFor;

import io.debezium.data.Envelope;

public class HeaderPartitionRoutingTest {

    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("server1.inventory.products.Value")
            .field("id", Schema.INT64_SCHEMA)
            .field("price", Schema.FLOAT32_SCHEMA)
            .field("product", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private final HeaderPartitionRouting<SourceRecord> PartitionRoutingTransformation = new HeaderPartitionRouting<>();

    @Test
    public void whenNoPartitionHeaderFieldDeclaredAConfigExceptionIsThrew() {

        assertThatThrownBy(() -> PartitionRoutingTransformation.configure(Map.of(
                "partition.topic.num", 2)))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value null for configuration partition.header.fields: The 'partition.header.fields' value is invalid: A value is required");
    }

    @Test
    public void whenNoPartitionTopicNumFieldDeclaredAConfigExceptionIsThrew() {

        assertThatThrownBy(() -> PartitionRoutingTransformation.configure(Map.of(
                "partition.header.fields", 2)))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value null for configuration partition.topic.num: The 'partition.topic.num' value is invalid: A value is required");
    }

    @Test
    public void whenPartitionHeaderFieldContainsEmptyElementAConfigExceptionIsThrew() {

        assertThatThrownBy(() -> PartitionRoutingTransformation.configure(Map.of(
                "partition.header.fields", ",source.table",
                "partition.topic.num", 2)))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        "Invalid value ,source.table for configuration partition.header.fields: The 'partition.header.fields' value is invalid: Empty string element(s) not permitted");
    }

    @Test
    public void correctComputeKafkaPartitionBasedOnNewConfiguredFieldOnCreateAndUpdateEvents() {

        PartitionRoutingTransformation.configure(Map.of(
                "partition.header.fields", "document-key-hash",
                "partition.topic.num", 2));

        Headers headers = new ConnectHeaders();
        headers.add("document-key-hash", "APPLE".getBytes(), null);

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "APPLE"), headers, CREATE);

        SourceRecord transformed = PartitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isZero();
    }

    @Test
    public void whenASpecifiedHeaderIsNotFoundOnHeaderItWillBeIgnored() {

        PartitionRoutingTransformation.configure(Map.of(
                "partition.header.fields", "not-exist",
                "partition.topic.num", 2));

        Headers headers = new ConnectHeaders();
        headers.add("document-key-hash", "APPLE".getBytes(), null);

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "APPLE"), headers, CREATE);

        SourceRecord transformed = PartitionRoutingTransformation.apply(eventRecord);

        assertThat(eventRecord).isEqualTo(transformed);
    }

    @Test
    @FixFor("DBZ-6543")
    public void whenNoHeadersIsSpecifiedItWillBeIgnored() {

        PartitionRoutingTransformation.configure(Map.of(
                "partition.header.fields", "document-key-hash",
                "partition.topic.num", 2));

        final SourceRecord eventRecord = buildSourceRecord(productRow(Map.of("id", 1L, "price", 1.0F)), CREATE);

        SourceRecord transformed = PartitionRoutingTransformation.apply(eventRecord);

        assertThat(eventRecord).isEqualTo(transformed);
    }

    @Test
    public void onlyFieldThatExistForCurrentEventWillBeUsedForPartitionComputation() {

        PartitionRoutingTransformation.configure(Map.of(
                "partition.header.fields", "document-key-hash,not-exist",
                "partition.topic.num", 3));

        Headers headers = new ConnectHeaders();
        headers.add("document-key-hash", "orange".getBytes(), null);

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "orange"), headers, CREATE);

        SourceRecord transformed1 = PartitionRoutingTransformation.apply(eventRecord);
       
        PartitionRoutingTransformation.configure(Map.of(
                "partition.header.fields", "document-key-hash",
                "partition.topic.num", 3));

        SourceRecord transformed2 = PartitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed1.kafkaPartition()).isEqualTo(1);
        assertThat(transformed2.kafkaPartition()).isEqualTo(1);
    }

    @Test
    public void correctComputeKafkaPartitionBasedOnSpecialChangeNestedFieldOnCreateDelete() {

        PartitionRoutingTransformation.configure(Map.of(
                "partition.header.fields", "document-key-hash",
                "partition.topic.num", 2));

        Headers headers = new ConnectHeaders();
        headers.add("document-key-hash", "APPLE".getBytes(), null);

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "APPLE"), headers, DELETE);

        SourceRecord transformed = PartitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isZero();
    }

    @Test
    public void truncateOperationRecordWillBeSkipped() {

        PartitionRoutingTransformation.configure(Map.of(
                "partition.header.fields", "change.product",
                "partition.topic.num", 2));

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "APPLE"), TRUNCATE);

        SourceRecord transformed = PartitionRoutingTransformation.apply(eventRecord);

        assertThat(eventRecord).isEqualTo(transformed);
    }

    @Test
    public void correctComputeKafkaPartitionBasedOnNotNestedField() {

        PartitionRoutingTransformation.configure(Map.of(
                "partition.header.fields", "document-key-string",
                "partition.topic.num", 100));

        Headers headers = new ConnectHeaders();
        headers.add("document-key-string", "ORANGE".getBytes(), null);

        Headers headersAlt = new ConnectHeaders();
        headersAlt.add("document-key-string", "APPLE".getBytes(), null);

        final SourceRecord createRecord1 = buildSourceRecord(productRow(1L, 1.0F, "APPLE"), headersAlt, CREATE);
        final SourceRecord createRecord2 = buildSourceRecord(productRow(1L, 1.0F, "ORANGE"), headers, CREATE);
        final SourceRecord updateRecord = buildSourceRecord(productRow(1L, 1.0F, "ORANGE"), headers, UPDATE);

        SourceRecord transformedCreateRecord1 = PartitionRoutingTransformation.apply(createRecord1);
        SourceRecord transformedCreateRecord2 = PartitionRoutingTransformation.apply(createRecord2);
        SourceRecord transformedUpdateRecord = PartitionRoutingTransformation.apply(updateRecord);

        assertThat(transformedCreateRecord1.kafkaPartition()).isNotEqualTo(transformedCreateRecord2.kafkaPartition());
        assertThat(transformedUpdateRecord.kafkaPartition()).isEqualTo(transformedCreateRecord2.kafkaPartition());
        assertThat(transformedUpdateRecord).isNotEqualTo(updateRecord);
    }

    @Test
    public void byDefaultJavaHashIsUsed() {

        PartitionRoutingTransformation.configure(Map.of(
                "partition.header.fields", "document-key-string",
                "partition.topic.num", 100));

        Headers headers = new ConnectHeaders();
        headers.add("document-key-string", "hello-world".getBytes(), null);

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "orange"), headers, CREATE);

        SourceRecord transformed = PartitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isEqualTo(67);
    }

    @Test
    public void murmurHashWillBeUsed() {

        PartitionRoutingTransformation.configure(Map.of(
                "partition.header.fields", "document-key-string",
                "partition.topic.num", 100,
                "partition.hash.function", "murmur"));

        Headers headers = new ConnectHeaders();
        headers.add("document-key-string", "hello-world".getBytes(), null);

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "orange"), headers, CREATE);

        SourceRecord transformed = PartitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isEqualTo(70);
    }


    @Test
    public void passthroughWillBeUsed() {

        PartitionRoutingTransformation.configure(Map.of(
                "partition.header.fields", "document-key-hash",
                "partition.topic.num", 100,
                "partition.hash.function", "passthrough"));

        Headers headers = new ConnectHeaders();
        headers.add("document-key-hash", "3898609789508776151".getBytes(), null);

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "orange"), headers, CREATE);

        SourceRecord transformed = PartitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isEqualTo(7);
    }

    @Test
    public void passthroughWillBeUsedAndMultiple() {

        PartitionRoutingTransformation.configure(Map.of(
                "partition.header.fields", "document-key-hash,document-key-hash2",
                "partition.topic.num", 100,
                "partition.hash.function", "passthrough"));

        Headers headers = new ConnectHeaders();
        headers.add("document-key-hash", "3898609789508776151".getBytes(), null);
        headers.add("document-key-hash2", "8054553072992330326".getBytes(), null);

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "orange"), headers, CREATE);

        SourceRecord transformed = PartitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isEqualTo(37);
    }

    @Test
    public void passthroughWillBeUsedAndMultipleOnlyExists() {

        PartitionRoutingTransformation.configure(Map.of(
                "partition.header.fields", "document-key-hash,no-exist",
                "partition.topic.num", 100,
                "partition.hash.function", "passthrough"));

        Headers headers = new ConnectHeaders();
        headers.add("document-key-hash", "3898609789508776151".getBytes(), null);
        headers.add("document-key-hash2", "8054553072992330326".getBytes(), null);

        final SourceRecord eventRecord = buildSourceRecord(productRow(1L, 1.0F, "orange"), headers, CREATE);

        SourceRecord transformed = PartitionRoutingTransformation.apply(eventRecord);

        assertThat(transformed.kafkaPartition()).isEqualTo(7);
    }

    private SourceRecord buildSourceRecord(Struct row, Headers headers, Envelope.Operation operation) {

        SchemaBuilder sourceSchemaBuilder = SchemaBuilder.struct()
                .name("source")
                .field("connector", Schema.STRING_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA);

        Schema sourceSchema = sourceSchemaBuilder.build();

        Envelope createEnvelope = Envelope.defineSchema()
                .withName("server1.inventory.product.Envelope")
                .withRecord(VALUE_SCHEMA)
                .withSource(sourceSchema)
                .build();

        Struct source = new Struct(sourceSchema);
        source.put("connector", "mysql");
        source.put("db", "inventory");
        source.put("table", "products");

        Struct payload = createEnvelope.create(row, source, Instant.now());

        switch (operation) {
            case CREATE:
            case UPDATE:
            case READ:
                payload = createEnvelope.create(row, source, Instant.now());
                break;
            case DELETE:
                payload = createEnvelope.delete(row, source, Instant.now());
                break;
            case TRUNCATE:
                payload = createEnvelope.truncate(source, Instant.now());
                break;
        }

        return new SourceRecord(
                new HashMap<>(), // source partition
                new HashMap<>(), // source offset
                "prefix.inventory.products",
                null, // No partition specified
                null, // No key schema
                null, // No key
                createEnvelope.schema(),
                payload,
                null, // No timestamp
                headers // Add headers to the SourceRecord
                );
    }

    private SourceRecord buildSourceRecord(Struct row, Envelope.Operation operation) {

        SchemaBuilder sourceSchemaBuilder = SchemaBuilder.struct()
                .name("source")
                .field("connector", Schema.STRING_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA);

        Schema sourceSchema = sourceSchemaBuilder.build();

        Envelope createEnvelope = Envelope.defineSchema()
                .withName("server1.inventory.product.Envelope")
                .withRecord(VALUE_SCHEMA)
                .withSource(sourceSchema)
                .build();

        Struct source = new Struct(sourceSchema);
        source.put("connector", "mysql");
        source.put("db", "inventory");
        source.put("table", "products");

        Struct payload = createEnvelope.create(row, source, Instant.now());

        switch (operation) {
            case CREATE:
            case UPDATE:
            case READ:
                payload = createEnvelope.create(row, source, Instant.now());
                break;
            case DELETE:
                payload = createEnvelope.delete(row, source, Instant.now());
                break;
            case TRUNCATE:
                payload = createEnvelope.truncate(source, Instant.now());
                break;
        }

        return new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "prefix.inventory.products",
                createEnvelope.schema(), payload
                );
    }

    private Struct productRow(long id, float price, String name) {

        return new Struct(VALUE_SCHEMA)
                .put("id", id)
                .put("price", price)
                .put("product", name);
    }

    private Struct productRow(Map<String, Object> rowValues) {

        Struct struct = new Struct(VALUE_SCHEMA);
        rowValues.forEach(struct::put);

        return struct;
    }
}
