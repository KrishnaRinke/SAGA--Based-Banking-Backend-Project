package com.bankapp.accountservice.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {

    private final AccountService accountService;


    ///consume transaction . completed event from kafka

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String,Object> payload){
       try{
           String receiveAccount = (String) payload.get("receiverAccountNumber");
           BigDecimal amount = new BigDecimal(payload.get("amount").toString());

           log.info("Crediting account: {} amount : {}",receiveAccount,amount);

           accountService.creditBalance(receiveAccount,amount);
       }
       catch(Exception e){
               log.error("Error Crediting amount : "+e.getMessage());

        }

    }

///Consume fraud detected event from the kafka bloks the flag account

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetection(@Payload Map<String,Object> payload){
        try {
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud Detected - Blocking the account: {}", accountNumber);

            accountService.blockAccount(accountNumber);
        }catch (Exception e){
             log.error("Error blocking account : {}",e.getMessage());
        }
    }
}
