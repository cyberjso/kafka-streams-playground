package io.joliveira;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.util.Properties;

import static java.util.Optional.ofNullable;

@Configuration
public class Config {

    @Bean
    public ObjectMapper mapper() {
        SimpleModule module = new SimpleModule();
        module.addSerializer(BigDecimal.class, new ToStringSerializer());
        ObjectMapper mapper =  new ObjectMapper();
        mapper.registerModule(module);
        mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        
        return mapper;
    }

    @Bean("streamConfig")
    public Properties streamsConfig() {
        String bootstrapServers = ofNullable(System.getProperty("bootstrap.servers", System.getenv("BOOTSTRAP_SERVERS")))
                .orElse("localhost:9092");

        String stateDir = ofNullable(System.getenv("STATE_DIR")).orElse("/tmp/kafka/customer-balance-state");

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "customer-balance-aggregations");
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10*1024);
        streamsConfiguration.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);

        return streamsConfiguration;
    }

    @Bean
    public StreamingApp streamingApp() {
        return new StreamingApp();
    }

    @Bean
    public Topology topology() {
        return new TopologyBuilder(mapper(), customerTransactionTopic(), customerBalanceTopic()).build();
    }

    @Bean
    public MeterRegistry meterRegistry() {
        // Uncomment this snippet in order to send these metrics to the local datadog agent using statsd
        /*    StatsdConfig config =
                new StatsdConfig() {
                    public String get(String k) {
                        return null;
                    }

                    public StatsdFlavor flavor() {
                        return StatsdFlavor.DATADOG;
                    }
                };

        return  new StatsdMeterRegistry(config, Clock.SYSTEM);*/

        return new SimpleMeterRegistry();
    }

    @Bean("customerTransactionTopic")
    public NewTopic customerTransactionTopic() {
        return new NewTopic("customer-transaction", 3, (short) 1);
    }

    @Bean("customerBalanceTopic")
    public NewTopic customerBalanceTopic() {
        return new NewTopic("customer-balance", 3, (short) 1);
    }

}
