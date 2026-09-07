package com.bankapp.paymentservice.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CreatePaymentRequest {
   @NotNull(message = "Account Number is required")
    private String accountNumber;

    @NotNull(message = "Amount is Required")
    @Positive(message = "Amount must be Positive")
    private BigDecimal amount;


    private String description;


}
