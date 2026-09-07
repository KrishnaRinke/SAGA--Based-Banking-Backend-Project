package com.bankapp.accountservice.dto;

import com.bankapp.accountservice.entity.AccountStatus;
import com.bankapp.accountservice.entity.AccountType;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AccountResponse {



    private String id;


    private String accountNumber;


    private String accountHolderName;


    private String email;


    private String phone;


    private AccountType accountType;


    private AccountStatus status;


    private BigDecimal balance;



    private BigDecimal dailyTransactionLimit;


    private LocalDateTime createdAt;


}
