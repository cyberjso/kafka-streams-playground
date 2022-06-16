package io.joliveira;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { TestConfig.class })
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TopologyBuilderTest {
    @Autowired
    private Topology topology;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    @Qualifier("customerTransactionTopic")
    private NewTopic customerTransactionTopic;

    @Autowired
    @Qualifier("customerBalanceTopic")
    private NewTopic customerBalanceTopic;

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;


    @BeforeEach
    public void setup() {
        topologyTestDriver = new TopologyTestDriver(topology);
        Serdes.StringSerde serdes = new Serdes.StringSerde();
        inputTopic = topologyTestDriver.createInputTopic(customerTransactionTopic.name(),  serdes.serializer(), serdes.serializer());
        outputTopic = topologyTestDriver.createOutputTopic(customerBalanceTopic.name(),  serdes.deserializer(), serdes.deserializer());
    }

    @AfterEach
    public void close() {
        topologyTestDriver.close();
    }

    @Test
    @DisplayName("Should aggregate transactions from the same customer")
    public void shouldAggregateTransactionsFromSameCustomer() {
        inputTopic.pipeInput("1", "{\"id\": \"abcdc\", \"customerId\": \"abc1\", \"customerName\": \"customer-1\", \"amount\": 1}");
        inputTopic.pipeInput("2", "{\"id\": \"abcdc\", \"customerId\": \"abc1\", \"customerName\": \"customer-1\", \"amount\": 2}");

        List<CustomerBalance> customers = outputTopic.readRecordsToList().stream().map(message -> {
            try {
                return mapper.readValue(message.getValue(), CustomerBalance.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        assertThat(customers, hasSize(2));
        CustomerBalance finalBalance = customers.get(1);
        assertThat(finalBalance.getCustomerId(), is("abc1") );
        assertThat(finalBalance.getCustomerName(), is("customer-1") );
        assertThat(finalBalance.getTotal(), is(new BigDecimal(3)) );
    }

    @Test
    @DisplayName("Should aggregate transactions from different customers")
    public void shouldAggregateTransactionsFromDifferentCustomers() {
        inputTopic.pipeInput("1", "{\"id\": \"abcdc\", \"customerId\": \"abc1\", \"customerName\": \"customer-1\", \"amount\": 1.5}");
        inputTopic.pipeInput("2", "{\"id\": \"abcdc\", \"customerId\": \"abc2\", \"customerName\": \"customer-2\", \"amount\": 2.5}");
        inputTopic.pipeInput("2", "{\"id\": \"abcdc\", \"customerId\": \"abc1\", \"customerName\": \"customer-1\", \"amount\": 2.5}");
        List<CustomerBalance> customers = outputTopic.readRecordsToList().stream().map(message -> {
            try {
                return mapper.readValue(message.getValue(), CustomerBalance.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        assertThat(customers, hasSize(3));
        CustomerBalance finalBalanceCustomer1 = customers.get(2);
        assertThat(finalBalanceCustomer1.getCustomerId(), is("abc1") );
        assertThat(finalBalanceCustomer1.getCustomerName(), is("customer-1") );
        assertThat(finalBalanceCustomer1.getTotal(), is(new BigDecimal(4.0).setScale(1)) );

        CustomerBalance finalBalanceCustomer2 = customers.get(1);
        assertThat(finalBalanceCustomer2.getCustomerId(), is("abc2") );
        assertThat(finalBalanceCustomer2.getCustomerName(), is("customer-2") );
        assertThat(finalBalanceCustomer2.getTotal(), is(new BigDecimal(2.5).setScale(1)) );

    }

}
