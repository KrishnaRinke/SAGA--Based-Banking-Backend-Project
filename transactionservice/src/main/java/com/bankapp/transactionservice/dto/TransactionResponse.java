package com.bankapp.transactionservice.dto;

import com.bankapp.transactionservice.entity.TrancsactionType;
import com.bankapp.transactionservice.entity.TransactionStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;


@AllArgsConstructor
@Data
@NoArgsConstructor
public class TransactionResponse {


    private String id;

    private String senderAccountNumber;

    private String receiverAccountNumber;

    private BigDecimal amount;


    private TrancsactionType type;


    private TransactionStatus status;

    private String description;

    private String failureReason;

    private String referenceNumber;

    private LocalDateTime createdAt;

    private LocalDateTime completedAt;
}
