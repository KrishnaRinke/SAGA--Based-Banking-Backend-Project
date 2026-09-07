package com.bankapp.frauddetectionservice.service;

import com.bankapp.frauddetectionservice.client.AccountServiceClient;
import com.bankapp.frauddetectionservice.model.FraudDetectionResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {

    private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private static final String FRAUD_CHECK_CLEAN_TOPIC = "fraud.check.clean";

    private final KafkaTemplate<String,Object> kafkaTemplate;

    private final RedisTemplate<String,String> redisTemplate;

   @Value("${fraud.suspicious-amount-multiplier}")
    private double suspiciousAmountMultiplier;

    @Value("${fraud.max-transaction-per-minute}")
    private int maxTransactionsPerMinutes;

    @Value("${fraud.max-balance-percentage}")
    private double maxBalancePercentage;

    private final AccountServiceClient accountServiceClient;
    public void checkTransaction(Map<String, Object> payload) {

         String transactionId = (String) payload.get("transactionId");
         String accountNumber = (String) payload.get("senderAccountNumber");
        BigDecimal amount = new BigDecimal(payload.get("amount").toString());


        //Fetch real balance from Account Service
        BigDecimal sendersBalance = accountServiceClient.getBalance(accountNumber);

        log.info("Checking transaction :{}    account: {}   amount: {}   balance: {}",transactionId,accountNumber,amount,sendersBalance);

         FraudDetectionResult result = performFraudCheck(accountNumber,amount,sendersBalance);

         //IF THE TRANSACTION FOUND SUSPECIOUS
         if(result.isFraud()){
             log.info("Suspecious activity detected in account : {}" + "reason : {} - requesting OTP verification ",accountNumber,result.getReason());


             Map<String,Object> verificationEvent = new HashMap<>();
             verificationEvent.put("transactionId",transactionId);
             verificationEvent.put("accountNumber",accountNumber);
             verificationEvent.put("amount",amount);
             verificationEvent.put("reason",result.getReason());

             kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC,transactionId,verificationEvent);
         }else{

             //TRANSACTION IS CLEAN GOOD TO GO
             log.info("Transaction is Clean");

             Map<String,Object> transactionCleanEvent = new HashMap<>();
             transactionCleanEvent.put("transactionId",transactionId);
             transactionCleanEvent.put("isFraud",false);
             transactionCleanEvent.put("reason",null);

             kafkaTemplate.send(FRAUD_CHECK_CLEAN_TOPIC,transactionId,transactionCleanEvent);

         }
    }

     private FraudDetectionResult performFraudCheck(String accountNumber, BigDecimal amount, BigDecimal sendersBalance) {


        //Check 1 : Velocity Check
         if(isVelocityExceeded(accountNumber)){
             return new FraudDetectionResult(true,"Too Many Transactions in 60 sec"+" - Velocity Limit Exceeded");
         }

         //check 2: Amount Check
         if(isAmountSuspecious(accountNumber,amount)){
             return new FraudDetectionResult(true,"Unusual Transaction Amount"+" - exceeds 3x your average");
         }


         //check 3:Balance check
         if(sendersBalance.compareTo(BigDecimal.ZERO)>0 && isBalanceCheckFailed(sendersBalance,amount)){
             return new FraudDetectionResult(true,"Transaction exceeds the 90% of the Account's amount");
         }

         return new FraudDetectionResult(false,null);
    }

    private boolean isBalanceCheckFailed(BigDecimal sendersBalance, BigDecimal amount) {

        BigDecimal maxAllowed = sendersBalance.multiply(BigDecimal.valueOf(maxBalancePercentage));

        log.info("Balance check - amount: {} maxAllowed : {} suspecious : {}",amount,maxAllowed,amount.compareTo(maxAllowed) > 0);

        return amount.compareTo(maxAllowed)>0;




    }

    private boolean isAmountSuspecious(String accountNumber, BigDecimal amount) {
        String avgKey = "fraud:avg_amount" + accountNumber;

        String avgStr = redisTemplate.opsForValue().get(avgKey);

        if(avgStr == null){
            redisTemplate.opsForValue().set(avgKey,amount.toString());
            return false;
        }

        BigDecimal avgAmount = new BigDecimal(avgStr);

        BigDecimal threshold = avgAmount.multiply(
                BigDecimal.valueOf(suspiciousAmountMultiplier)
        );

        //update running average

        BigDecimal newAvg = avgAmount.add(amount).divide(BigDecimal.valueOf(2),2, RoundingMode.HALF_UP);

        redisTemplate.opsForValue().set(avgKey,newAvg.toString());

        log.info("Amount check - amount {} threshold {} suspicious {} ",amount,threshold,amount.compareTo(threshold)>0);


        return amount.compareTo(threshold) > 0;
    }

    private boolean isVelocityExceeded(String accountNumber) {

        String key = "fraud.velocity" + accountNumber;

        Long count = redisTemplate.opsForValue().increment(key);

        if(count != null && count == 1){
              redisTemplate.expire(key,60, TimeUnit.SECONDS);
        }

        log.info("Velocity check - account : {} count: {}/{}",accountNumber,count,maxTransactionsPerMinutes);

        return count!= null && count > maxTransactionsPerMinutes;
    }

}
