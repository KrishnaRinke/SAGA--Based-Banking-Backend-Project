package com.bankapp.accountservice.service;

import com.bankapp.accountservice.dto.AccountResponse;
import com.bankapp.accountservice.dto.CreateAccountRequest;
import com.bankapp.accountservice.entity.Account;
import com.bankapp.accountservice.entity.AccountStatus;
import com.bankapp.accountservice.entity.AccountType;
import com.bankapp.accountservice.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;

    private static SecureRandom secureRandom = new SecureRandom();


    public AccountResponse createAccount(CreateAccountRequest createAccountRequest) {
         log.info("Creating account for {}", createAccountRequest.getEmail());

         boolean isAvailable = accountRepository.existsByEmail(createAccountRequest.getEmail());

         if(isAvailable){
             throw new RuntimeException("Account Alredy exists for this email :" + createAccountRequest.getEmail());
         }

         Account account = new Account();

         account.setAccountHolderName(createAccountRequest.getAccountHolderName());
         account.setEmail(createAccountRequest.getEmail());
         account.setPhone(createAccountRequest.getPhone());
         account.setAccountType(createAccountRequest.getAccountType());
         account.setStatus(AccountStatus.ACTIVE);
         account.setBalance(createAccountRequest.getInitialDeposit());
         account.setAccountNumber(generateAccountNumber());
         account.setDailyTransactionLimit(
                 createAccountRequest.getAccountType() == AccountType.SAVINGS
                 ? new BigDecimal("100000")
                 : new BigDecimal("500000")
         );

         Account savedAccount = accountRepository.save(account);

         log.info("Account Created :" + savedAccount.getAccountNumber());

         return mapToResponse(savedAccount);



    }


    ////logic for the account number generation
    private String generateAccountNumber() {

         String accountNumber;

         do{
             long number = secureRandom.nextLong(1_000_000_000_000l);

             accountNumber = String.format("%012d",number);

         }while(accountRepository.existsByAccountNumber(accountNumber));

         return accountNumber;
    }




    public AccountResponse getAccount(String accountNumber) {
         Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not Found"));

        return mapToResponse(account);
    }

    public BigDecimal getBalance(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not Found,unable to fetch the balance"));
        return account.getBalance();
    }


    //////called by the kafka for the fraud detection
    public void blockAccount(String accountNumber) {

       log.info("Blocking Account....");

        Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not Found,unable to fetch the balance"));

        account.setStatus(AccountStatus.BLOCKED);

        accountRepository.save(account);

        log.info("Account blocked :" + account.getAccountNumber());

    }


    //////////////////saga steps 1st and 4th/////////////////////////////


    /////called by transaction service
    ////deduct balance from sender account
    public void deductBalance(String accountNumber, BigDecimal amount) {
      log.info("deducting balance {} from account : {}",amount , accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not Found,unable to fetch the balance"));

        if(account.getStatus() != AccountStatus.ACTIVE){
            throw new RuntimeException("Account is Inactive " + accountNumber);
        }

        if(account.getBalance().compareTo(amount) < 0){
            throw new RuntimeException("Insufficient funds for account :"+accountNumber);
        }

        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);

        log.info("Balance Updated ,new balance is : "+ account.getBalance());

    }



    /////called by the kafka
    public void creditBalance(String accountNumber, BigDecimal amount) {

       log.info("Crediting {} to account: {}",amount,accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(() -> new RuntimeException("Account not Found,unable to fetch the balance"));

        account.setBalance(account.getBalance().add(amount));

        accountRepository.save(account);

        log.info("Balance Credited, New Balance : {}",account.getBalance());

    }


    public AccountResponse mapToResponse(Account savedAccount){
        AccountResponse accountResponse = new AccountResponse();

        accountResponse.setAccountNumber(savedAccount.getAccountNumber());
        accountResponse.setAccountType(savedAccount.getAccountType());
        accountResponse.setAccountHolderName(savedAccount.getAccountHolderName());
        accountResponse.setBalance(savedAccount.getBalance());
        accountResponse.setId(savedAccount.getId());
        accountResponse.setEmail(savedAccount.getEmail());
        accountResponse.setDailyTransactionLimit(savedAccount.getDailyTransactionLimit());
        accountResponse.setCreatedAt(savedAccount.getCreatedAt());
        accountResponse.setStatus(savedAccount.getStatus());
        accountResponse.setPhone(savedAccount.getPhone());

        return accountResponse;

    }


}
