package com.bankapp.transactionservice.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;


//Bas ek cheez yaad rakh:
//
//Feign → simple synchronous service-to-service calls ke liye bahut convenient
//WebClient → reactive/non-blocking calls aur zyada control chahiye toh useful

@FeignClient(name = "accountservice",url = "${account.service.url}")
public interface AccountServiceClient {

    @PutMapping("/api/v1/accounts/{accountNumber}/deduct")
     public String deductBalance(
            @PathVariable String accountNumber,
            @RequestParam BigDecimal amount
            );

    @PutMapping("/api/v1/accounts/{accountNumber}/credit")
    String creditBalance(
            @PathVariable String accountNumber,
            @RequestParam BigDecimal amount
    );

}
