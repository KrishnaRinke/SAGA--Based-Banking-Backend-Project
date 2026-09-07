package com.bankapp.transactionservice.dto;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransactionRequest {

   @NotNull(message = "Please Provide Your Account Number")
    private String senderAccountNumber;

    @NotNull(message = "Please Provide Receivers Account Number")
    private String receiverAccountNumber;

    @NotNull(message = "Amount is Required")
    @Positive(message = "Amount Must be positive")
    private BigDecimal amount;

//    @Enumerated(EnumType.STRING)
//    @Column(nullable = false)
//    private TrancsactionType type;


    private String description;


}
