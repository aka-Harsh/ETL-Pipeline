package com.flowforge.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class RabbitMQConfig {

    // Exchange names
    public static final String TASKS_EXCHANGE = "flowforge.tasks";
    public static final String EVENTS_EXCHANGE = "flowforge.events";
    public static final String DLX_EXCHANGE = "flowforge.dlx";

    // Queue names
    public static final String QUEUE_S3_SOURCE = "flowforge.tasks.s3source";
    public static final String QUEUE_KAFKA_SOURCE = "flowforge.tasks.kafkasource";
    public static final String QUEUE_FILTER = "flowforge.tasks.filter";
    public static final String QUEUE_MAPPER = "flowforge.tasks.mapper";
    public static final String QUEUE_AGGREGATOR = "flowforge.tasks.aggregator";
    public static final String QUEUE_PG_SINK = "flowforge.tasks.pgsink";
    public static final String QUEUE_S3_SINK = "flowforge.tasks.s3sink";
    public static final String QUEUE_KAFKA_SINK = "flowforge.tasks.kafkasink";
    public static final String QUEUE_EVENTS_COMPLETED = "flowforge.events.completed";
    public static final String QUEUE_EVENTS_FAILED = "flowforge.events.failed";
    public static final String QUEUE_DLQ = "flowforge.dlq";
    public static final String QUEUE_NOTIFICATIONS = "flowforge.notifications";

    // Routing keys
    public static final String RK_S3_SOURCE = "node.s3source";
    public static final String RK_KAFKA_SOURCE = "node.kafkasource";
    public static final String RK_FILTER = "node.filter";
    public static final String RK_MAPPER = "node.mapper";
    public static final String RK_AGGREGATOR = "node.aggregator";
    public static final String RK_PG_SINK = "node.pgsink";
    public static final String RK_S3_SINK = "node.s3sink";
    public static final String RK_KAFKA_SINK = "node.kafkasink";
    public static final String RK_COMPLETED = "node.completed";
    public static final String RK_FAILED = "node.failed";

    // ─── Exchanges ───

    @Bean
    public DirectExchange tasksExchange() {
        return ExchangeBuilder.directExchange(TASKS_EXCHANGE).durable(true).build();
    }

    @Bean
    public TopicExchange eventsExchange() {
        return ExchangeBuilder.topicExchange(EVENTS_EXCHANGE).durable(true).build();
    }

    @Bean
    public FanoutExchange dlxExchange() {
        return ExchangeBuilder.fanoutExchange(DLX_EXCHANGE).durable(true).build();
    }

    // ─── DLQ (must be declared before task queues reference it) ───

    @Bean
    public Queue dlqQueue() {
        return QueueBuilder.durable(QUEUE_DLQ).build();
    }

    @Bean
    public Binding dlqBinding() {
        return BindingBuilder.bind(dlqQueue()).to(dlxExchange());
    }

    // ─── Task Queues ───

    private Map<String, Object> taskQueueArgs() {
        return Map.of(
                "x-dead-letter-exchange", DLX_EXCHANGE,
                "x-message-ttl", 300000
        );
    }

    @Bean public Queue s3SourceQueue()    { return QueueBuilder.durable(QUEUE_S3_SOURCE).withArguments(taskQueueArgs()).build(); }
    @Bean public Queue kafkaSourceQueue() { return QueueBuilder.durable(QUEUE_KAFKA_SOURCE).withArguments(taskQueueArgs()).build(); }
    @Bean public Queue filterQueue()      { return QueueBuilder.durable(QUEUE_FILTER).withArguments(taskQueueArgs()).build(); }
    @Bean public Queue mapperQueue()      { return QueueBuilder.durable(QUEUE_MAPPER).withArguments(taskQueueArgs()).build(); }
    @Bean public Queue aggregatorQueue()  { return QueueBuilder.durable(QUEUE_AGGREGATOR).withArguments(taskQueueArgs()).build(); }
    @Bean public Queue pgSinkQueue()      { return QueueBuilder.durable(QUEUE_PG_SINK).withArguments(taskQueueArgs()).build(); }
    @Bean public Queue s3SinkQueue()      { return QueueBuilder.durable(QUEUE_S3_SINK).withArguments(taskQueueArgs()).build(); }
    @Bean public Queue kafkaSinkQueue()   { return QueueBuilder.durable(QUEUE_KAFKA_SINK).withArguments(taskQueueArgs()).build(); }

    // ─── Event Queues ───

    @Bean public Queue eventsCompletedQueue() { return QueueBuilder.durable(QUEUE_EVENTS_COMPLETED).build(); }
    @Bean public Queue eventsFailedQueue()    { return QueueBuilder.durable(QUEUE_EVENTS_FAILED).build(); }
    @Bean public Queue notificationsQueue()   { return QueueBuilder.durable(QUEUE_NOTIFICATIONS).build(); }

    // ─── Bindings ───

    @Bean public Binding s3SourceBinding()    { return BindingBuilder.bind(s3SourceQueue()).to(tasksExchange()).with(RK_S3_SOURCE); }
    @Bean public Binding kafkaSourceBinding() { return BindingBuilder.bind(kafkaSourceQueue()).to(tasksExchange()).with(RK_KAFKA_SOURCE); }
    @Bean public Binding filterBinding()      { return BindingBuilder.bind(filterQueue()).to(tasksExchange()).with(RK_FILTER); }
    @Bean public Binding mapperBinding()      { return BindingBuilder.bind(mapperQueue()).to(tasksExchange()).with(RK_MAPPER); }
    @Bean public Binding aggregatorBinding()  { return BindingBuilder.bind(aggregatorQueue()).to(tasksExchange()).with(RK_AGGREGATOR); }
    @Bean public Binding pgSinkBinding()      { return BindingBuilder.bind(pgSinkQueue()).to(tasksExchange()).with(RK_PG_SINK); }
    @Bean public Binding s3SinkBinding()      { return BindingBuilder.bind(s3SinkQueue()).to(tasksExchange()).with(RK_S3_SINK); }
    @Bean public Binding kafkaSinkBinding()   { return BindingBuilder.bind(kafkaSinkQueue()).to(tasksExchange()).with(RK_KAFKA_SINK); }

    @Bean public Binding completedBinding() { return BindingBuilder.bind(eventsCompletedQueue()).to(eventsExchange()).with(RK_COMPLETED); }
    @Bean public Binding failedBinding()    { return BindingBuilder.bind(eventsFailedQueue()).to(eventsExchange()).with(RK_FAILED); }

    // ─── JSON Message Converter ───

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter());
        return template;
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jsonMessageConverter());
        factory.setPrefetchCount(1);
        return factory;
    }
}
