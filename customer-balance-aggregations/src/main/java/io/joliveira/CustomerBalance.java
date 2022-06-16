package io.joliveira;

import java.math.BigDecimal;

public class CustomerBalance {
    private String  customerId;
    private String customerName;
    private BigDecimal total;

    public CustomerBalance() {}

    public CustomerBalance(String customerId, String customerName, BigDecimal total) {
        this.customerId = customerId;
        this.customerName = customerName;
        this.total = total;
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
    public void setTotal(BigDecimal total) {
        this.total = total;
    }
    public BigDecimal getTotal() {
        return total;
    }
}
