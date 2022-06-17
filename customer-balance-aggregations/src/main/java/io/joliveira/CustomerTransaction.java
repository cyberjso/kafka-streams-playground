package io.joliveira;

import java.math.BigDecimal;

public class CustomerTransaction {
    private String id;
    private String customerId;
    private String customerName;
    private BigDecimal amount;

    public CustomerTransaction() {}

    public CustomerTransaction(String id, String customerId, String customerName,  BigDecimal amount) {
        this.id = id;
        this.customerId =  customerId;
        this.customerName = customerName;
        this.amount = amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }
    public BigDecimal getAmount() {
        return amount;
    }
    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }
    public String getCustomerId() {
        return customerId;
    }
    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }
    public String getCustomerName() {
        return customerName;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
