package io.joliveira;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;

import static java.util.Optional.ofNullable;

class TopologyBuilder {
    private ObjectMapper mapper;
    private NewTopic customerTransactionTopic;
    private NewTopic customerBalanceTopic;

    public TopologyBuilder(ObjectMapper mapper, NewTopic customerTransactionTopic, NewTopic customerBalanceTopic) {
        this.mapper =  mapper;
        this.customerBalanceTopic  =customerBalanceTopic;
        this.customerTransactionTopic =  customerTransactionTopic;
    }

    public Topology build() {
        Serde<String> defaultIdSerde =   Serdes.String();

        JsonDeserializer transactionDeserializer =  new JsonDeserializer(mapper, CustomerTransaction.class);
        JsonSerializer<CustomerTransaction> transactionSerializer =  new JsonSerializer<>(mapper);
        Serde<CustomerTransaction> transactionSerde =  Serdes.serdeFrom(transactionSerializer, transactionDeserializer);

        JsonSerializer<CustomerBalance> customerBalanceJsonSerializer = new JsonSerializer<>(mapper);
        JsonDeserializer customerBalanceJsonDeserializer = new JsonDeserializer(mapper, CustomerBalance.class);
        Serde<CustomerBalance> customerBalanceSerde =  Serdes.serdeFrom(customerBalanceJsonSerializer, customerBalanceJsonDeserializer);

        return  buildTopology(defaultIdSerde, transactionSerde, customerBalanceSerde);
    }

    private Topology buildTopology(Serde<String> idSerde, Serde<CustomerTransaction> transactionSerde, Serde<CustomerBalance> customerBalanceSerde) {
        StreamsBuilder streamBuilder = new StreamsBuilder();
        KStream<String, CustomerTransaction> customerTransactions =  streamBuilder
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

        return streamBuilder.build();
    }

}
