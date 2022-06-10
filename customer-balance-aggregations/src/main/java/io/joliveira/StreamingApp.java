package io.joliveira;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

@Component
public class StreamingApp {
    private static Logger logger = LoggerFactory.getLogger(StreamingApp.class);

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    @Qualifier("streamConfig")
    private Properties streamProperties;

    @Autowired
    @Qualifier("customerTransactionTopic")
    private NewTopic customerTransactionTopic;

    @Autowired
    @Qualifier("customerTransactionTopic")
    private NewTopic customerBalanceTopic;

    @Autowired
    private MeterRegistry registry;

    @PostConstruct
    public void init() {
        createTopics(List.of(customerBalanceTopic, customerTransactionTopic));

        KafkaStreams streams = start();
        streams.start();

        KafkaStreamsMetrics kafkaStreamsMetrics = new KafkaStreamsMetrics(streams);
        kafkaStreamsMetrics.bindTo(registry);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        Runtime.getRuntime().addShutdownHook(new Thread(registry::close));

        // Printing out all metrics available
        registry.forEachMeter(meter -> logger.info(meter.getId().toString()));
    }

    private KafkaStreams start() {
        Serde<String> defaultIdSerde =   Serdes.String();

        JsonDeserializer transactionDeserializer =  new JsonDeserializer(mapper, CustomerTransaction.class);
        JsonSerializer<CustomerTransaction> transactionSerializer =  new JsonSerializer<>(mapper);
        Serde<CustomerTransaction> transactionSerde =  Serdes.serdeFrom(transactionSerializer, transactionDeserializer);

        JsonSerializer<CustomerBalance> customerBalanceJsonSerializer = new JsonSerializer<>(mapper);
        JsonDeserializer customerBalanceJsonDeserializer = new JsonDeserializer(mapper, CustomerBalance.class);
        Serde<CustomerBalance> customerBalanceSerde =  Serdes.serdeFrom(customerBalanceJsonSerializer, customerBalanceJsonDeserializer);

        Topology topology = buildTopology(defaultIdSerde, transactionSerde, customerBalanceSerde);

        return new KafkaStreams(topology, streamProperties);
    }

    private Topology buildTopology(Serde<String> idSerde, Serde<CustomerTransaction> transactionSerde, Serde<CustomerBalance> customerBalanceSerde) {
        StreamsBuilder topologyBuilder = new StreamsBuilder();
        KStream<String, CustomerTransaction> customerTransactions =  topologyBuilder
                .stream(customerTransactionTopic.name(), Consumed.with(idSerde, transactionSerde))
                .map(((key, value) -> new KeyValue<>(value.getCustomerId(), value)));

        KTable<String, CustomerBalance> customerBalance = customerTransactions
                                                            .groupByKey(Grouped.with(idSerde, transactionSerde))
                                                            .aggregate( () -> new CustomerBalance(),
                                                                        (customerId, currentTransaction,  balance ) ->  {
                                                                            BigDecimal total = ofNullable(currentTransaction.getAmount()).orElse(new BigDecimal(0l));
                                                                            BigDecimal current = ofNullable(balance.getTotal()).orElse(new BigDecimal(0l));
                                                                            balance.setTotal(current.add(total));
                                                                            balance.setCustomerId(currentTransaction.getCustomerId());
                                                                            balance.setCustomerName(currentTransaction.getCustomerName());

                                                                            return balance;
                                                                        },
                                                                        Materialized.<String, CustomerBalance, KeyValueStore<Bytes, byte[]>>
                                                                                    as("customer_balance")
                                                                            .withKeySerde(idSerde)
                                                                            .withValueSerde(customerBalanceSerde));

        customerBalance
                .toStream()
                .to(customerBalanceTopic.name(), Produced.with(idSerde, customerBalanceSerde));

        return topologyBuilder.build();
    }

    private void createTopics(List<NewTopic> topics) {
        String bootstrapServers = ofNullable(System.getenv("BOOTSTRAP_SERVERS")).orElse("localhost:9092");
        final Map<String, Object> props = Map.of(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                    AdminClientConfig.RETRIES_CONFIG, 5);


        try (AdminClient adminClient = AdminClient.create(props)) {
            List<String> currentTopics = adminClient.listTopics().names().get().stream().collect(Collectors.toList());

            // Check whether topics already exists or not
            List<NewTopic> newTopics = ofNullable(topics)
                                        .orElse(new ArrayList<>())
                                        .stream()
                                        .filter(topic -> !currentTopics.contains(topic.name()))
                                        .collect(Collectors.toList());
            CreateTopicsResult result = adminClient.createTopics(newTopics);
            result.all().get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic", e);
        }
    }

}