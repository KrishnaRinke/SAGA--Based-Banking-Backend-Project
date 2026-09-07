package com.bankapp.accountservice.controller;


import com.bankapp.accountservice.dto.AccountResponse;
import com.bankapp.accountservice.dto.CreateAccountRequest;
import com.bankapp.accountservice.service.AccountService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

@RestController
@RequestMapping("/api/v1/accounts")
@Slf4j
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;


    @PostMapping
    public ResponseEntity<AccountResponse> createAccount(@Valid  @RequestBody CreateAccountRequest createAccountRequest){

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(accountService.createAccount(createAccountRequest));

    }

    @GetMapping("/{accountNumber}")
    public ResponseEntity<AccountResponse> getAccount(@PathVariable String accountNumber){

        return ResponseEntity.ok(accountService.getAccount(accountNumber));
    }

    @GetMapping("/{accountNumber}/balance")
    public ResponseEntity<BigDecimal> getBalance(@PathVariable String accountNumber){

        return ResponseEntity.ok(accountService.getBalance(accountNumber));
    }

    @PutMapping("/{accountNumber}/block")
    public ResponseEntity<String> blockAccount(@PathVariable String accountNumber){
        accountService.blockAccount(accountNumber);
        return  ResponseEntity.ok("ACCOUNT BLOCKED SUCCESSFULLY.");
    }



    //Saga step 1 of saga
    //called by transaction  service when the transfer is initiated
    //
   @PutMapping("/{accountNumber}/deduct")
    public ResponseEntity<String> deductBalance(@PathVariable String accountNumber,@RequestParam BigDecimal amount){
        accountService.deductBalance(accountNumber,amount);

        return ResponseEntity.ok("BALANCE DEDUCTED SUCCESSFULLY");
    }


    //Saga step 4
    //Compensating and transaction endpoint
    //called by transaction in 2 scenarios 1.fraud detected-(refund sender(undo step 1))
    //                                     2.transaction completed -> credit receiver

    @PutMapping("/{accountNumber}/credit")
    public ResponseEntity<String> creditBalance(@PathVariable String accountNumber,@RequestParam BigDecimal amount){
        accountService.creditBalance(accountNumber,amount);
        return ResponseEntity.ok("BALANCE CREDICTED SUCCESSFULLY");
    }


}
