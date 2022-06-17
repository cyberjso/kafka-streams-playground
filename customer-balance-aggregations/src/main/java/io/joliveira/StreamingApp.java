package io.joliveira;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
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
    private Topology topology;

    @Autowired
    @Qualifier("streamConfig")
    private Properties streamProperties;

    @Autowired
    @Qualifier("customerTransactionTopic")
    private NewTopic customerTransactionTopic;

    @Autowired
    @Qualifier("customerBalanceTopic")
    private NewTopic customerBalanceTopic;

    @Autowired
    private MeterRegistry registry;

    private KafkaStreams kafkaStreams;

    public StreamingApp() { }

    @PostConstruct
    public void init() {
        createTopics(List.of(customerBalanceTopic, customerTransactionTopic));

        kafkaStreams = new KafkaStreams(topology, streamProperties);
        kafkaStreams.start();

        KafkaStreamsMetrics kafkaStreamsMetrics = new KafkaStreamsMetrics(kafkaStreams);
        kafkaStreamsMetrics.bindTo(registry);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        Runtime.getRuntime().addShutdownHook(new Thread(registry::close));

        // Printing out all metrics available
        registry.forEachMeter(meter -> logger.info(meter.getId().toString()));
    }

    public void stop() {
        kafkaStreams.close();
        registry.close();
    }

    private void createTopics(List<NewTopic> topics) {
        String bootstrapServers = ofNullable(System.getProperty("bootstrap.servers", System.getenv("BOOTSTRAP_SERVERS")))
                                    .orElse("localhost:9092");

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
            if (newTopics.size() > 0) {
                CreateTopicsResult result = adminClient.createTopics(newTopics);
                result.all().get();
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic", e);
        }
    }

}