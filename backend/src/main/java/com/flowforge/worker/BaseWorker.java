package com.flowforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.model.CompletionEvent;
import com.flowforge.model.TaskMessage;
import com.flowforge.rabbitmq.TaskPublisher;
import com.flowforge.service.StatusService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;

import java.time.Duration;
import java.util.*;

/**
 * Base class providing common Kafka consumer/producer utilities to all workers.
 */
@RequiredArgsConstructor
@Slf4j
public abstract class BaseWorker implements NodeExecutor {

    public abstract long execute(TaskMessage task) throws Exception;


    protected final TaskPublisher taskPublisher;
    protected final StatusService statusService;
    protected final ObjectMapper objectMapper;

    @Value("${spring.kafka.bootstrap-servers}")
    protected String bootstrapServers;

    /**
     * Creates a raw Kafka consumer subscribed to the given topics.
     * Uses earliest offset so it reads all records from the beginning.
     */
    protected KafkaConsumer<String, String> createConsumer(List<String> topics, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);
        return consumer;
    }

    /**
     * Creates a Kafka producer.
     */
    protected KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    /**
     * Drains all records from the given Kafka topics.
     * Waits for partition assignment (handles slow rebalance), seeks to beginning,
     * then stops after 3 consecutive empty polls.
     */
    protected List<String> drainTopics(List<String> topics, String groupId) {
        List<String> records = new ArrayList<>();
        if (topics == null || topics.isEmpty()) return records;

        try (KafkaConsumer<String, String> consumer = createConsumer(topics, groupId)) {
            // Wait up to 30s for partition assignment — rebalance can take 3–5 seconds
            int assignWaits = 0;
            while (consumer.assignment().isEmpty() && assignWaits < 30) {
                consumer.poll(Duration.ofMillis(1000));
                assignWaits++;
            }
            if (consumer.assignment().isEmpty()) {
                log.warn("No partition assignment after 30s for topics: {}", topics);
                return records;
            }
            // Seek to the very beginning so we always read all records, regardless of committed offsets
            consumer.seekToBeginning(consumer.assignment());

            int emptyPolls = 0;
            while (emptyPolls < 3) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(2000));
                if (batch.isEmpty()) {
                    emptyPolls++;
                } else {
                    emptyPolls = 0;
                    for (ConsumerRecord<String, String> record : batch) {
                        records.add(record.value());
                    }
                    consumer.commitSync();
                }
            }
        }
        log.debug("Drained {} records from topics {}", records.size(), topics);
        return records;
    }

    /**
     * Publishes a list of JSON record strings to the given Kafka topics.
     */
    protected void publishToTopics(List<String> records, List<String> topics) {
        if (topics == null || topics.isEmpty()) return;
        try (KafkaProducer<String, String> producer = createProducer()) {
            for (String record : records) {
                for (String topic : topics) {
                    producer.send(new ProducerRecord<>(topic, record));
                }
            }
            producer.flush();
        }
    }

    /**
     * Publishes a success completion event to RabbitMQ.
     */
    protected void publishSuccess(TaskMessage task, long records, long durationMs) {
        CompletionEvent event = new CompletionEvent();
        event.setExecutionId(task.getExecutionId());
        event.setPipelineId(task.getPipelineId());
        event.setNodeId(task.getNodeId());
        event.setNodeType(task.getNodeType());
        event.setSuccess(true);
        event.setRecordsProcessed(records);
        event.setDurationMs(durationMs);
        taskPublisher.publishCompletion(event);
    }

    /**
     * Publishes a failure completion event to RabbitMQ.
     */
    protected void publishFailure(TaskMessage task, String error) {
        CompletionEvent event = new CompletionEvent();
        event.setExecutionId(task.getExecutionId());
        event.setPipelineId(task.getPipelineId());
        event.setNodeId(task.getNodeId());
        event.setNodeType(task.getNodeType());
        event.setSuccess(false);
        event.setErrorMessage(error);
        taskPublisher.publishCompletion(event);
    }
}
