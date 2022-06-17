package io.joliveira;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { Config.class })
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StreamingAppIntegrationTest {
    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private StreamingApp streamingApp;

    @Autowired
    @Qualifier("customerTransactionTopic")
    private NewTopic customerTransactionTopic;

    @Autowired
    @Qualifier("customerBalanceTopic")
    private NewTopic customerBalanceTopic;

    private static KafkaContainer kafka;
    private Consumer<String, String> kafkaConsumer;
    private Producer<String, String> kafkaProducer;

    static  {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
        kafka.start();
        System.setProperty("bootstrap.servers", kafka.getBootstrapServers());
    }

    @BeforeAll
    public void init() {
        kafkaProducer = createProducer(kafka.getBootstrapServers());
        kafkaConsumer= createConsumer(kafka.getBootstrapServers());
    }

    @AfterAll
    public void tearDown() {
        streamingApp.stop();
        kafkaProducer.close();
        kafkaConsumer.close();
        kafka.close();
    }

    @Test
    @DisplayName("Should create Customer balance topic with partitions and replication factor")
    public void shouldCreateCustomerBalanceTopic() {
        TopicDescription topicDescription = describe(customerBalanceTopic.name());
        Integer replicas  = topicDescription.partitions().get(0).replicas().size();

        assertThat(topicDescription.partitions().size(), is(topicDescription.partitions().size()));
        assertThat(replicas, is(Integer.valueOf(customerBalanceTopic.replicationFactor())));
    }

    @Test
    @DisplayName("Should create Customer Transaction topic with partitions and replication factor")
    public void shouldCreateCustomerTransactionTopic() {
        TopicDescription topicDescription = describe(customerTransactionTopic.name());
        Integer replicas  = topicDescription.partitions().get(0).replicas().size();

        assertThat(topicDescription.partitions().size(), is(topicDescription.partitions().size()));
        assertThat(replicas, is(Integer.valueOf(customerTransactionTopic.replicationFactor())));
    }

    @Test
    @DisplayName("Should return an aggregated message when sending a customer transaction")
    public void shouldReturnAnAggregatedMessageWhenSendingCustomerTransaction() throws JsonProcessingException {
        String customerId = UUID.randomUUID().toString();
        String customerName = "customer-" + customerId;
        CustomerTransaction customerTransaction =  new CustomerTransaction("1", customerId, customerName, new BigDecimal(1).setScale(2));
        kafkaProducer.send(new ProducerRecord<>(customerTransactionTopic.name(), customerTransaction.getId(), mapper.writeValueAsString(customerTransaction)));

        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(5));

        assertThat(consumerRecords.count(), is(1));
        ConsumerRecord<String, String> record = consumerRecords.iterator().next();
        CustomerBalance balance = mapper.readValue(record.value(), CustomerBalance.class);
        assertThat(balance.getCustomerId(), is(customerId));
        assertThat(balance.getCustomerName(), is(customerName));
        assertThat(balance.getTotal(), is(new BigDecimal(1).setScale(2)));
    }

    private Consumer<String, String> createConsumer(String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "customer-balance-aggregation-integ-test-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(customerBalanceTopic.name()));

        return consumer;
    }

    private Producer<String, String> createProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "customer-balance-aggregation-integ-test-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }
    
    private TopicDescription  describe(String topicName) {
        final Map<String, Object> props = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                AdminClientConfig.RETRIES_CONFIG, 5);

        try (AdminClient adminClient = AdminClient.create(props)) {
            DescribeTopicsResult topicsResult = adminClient.describeTopics(List.of(customerBalanceTopic.name(), customerTransactionTopic.name()));

            return  topicsResult.topicNameValues().get(customerBalanceTopic.name()).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
