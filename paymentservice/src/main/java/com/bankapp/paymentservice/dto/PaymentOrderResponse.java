package com.bankapp.paymentservice.dto;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.math.BigInteger;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentOrderResponse {
    private String paymentId;

    private String razorpayOrderId;

    private BigDecimal amount;

    private String currency;

    private String status;

    private String razorpayId;
}
