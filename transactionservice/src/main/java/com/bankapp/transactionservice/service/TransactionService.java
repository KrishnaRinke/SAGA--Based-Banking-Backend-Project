package com.bankapp.transactionservice.service;

import com.bankapp.transactionservice.client.AccountServiceClient;
import com.bankapp.transactionservice.dto.TransactionRequest;
import com.bankapp.transactionservice.dto.TransactionResponse;
import com.bankapp.transactionservice.entity.TrancsactionType;
import com.bankapp.transactionservice.entity.Transaction;
import com.bankapp.transactionservice.entity.TransactionStatus;
import com.bankapp.transactionservice.event.TransactionCompletedEvent;
import com.bankapp.transactionservice.event.TransactionInitiatedEvent;
import com.bankapp.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final TransactionRepository transactionRepository;
    private final RedisTemplate<String,String> redisTemplate;

    private final AccountServiceClient accountServiceClient;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";

    private static final String FRAUD_DETECTED_TOPIC = "fraud.detection";

    public TransactionResponse getTransaction(String transactionId) {


        return mapToResponse(transactionRepository.findById(transactionId).orElseThrow(()-> new RuntimeException("The Transition with this Id Do not exist.")));

    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber) {

        return transactionRepository.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber).stream().map(this::mapToResponse).collect(Collectors.toList());
    }

    public TransactionResponse verifyOTP(String transactionId, String otp) {

        log.info("OTP verification for the transaction : {}",transactionId);

        Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(()-> new RuntimeException("Transaction not Found"));

        if (transaction.getStatus() != TransactionStatus.PENDING_VERIFICATION) {

            log.warn(
                    "OTP verification rejected - transaction {} is already {}",
                    transactionId,
                    transaction.getStatus()
            );

            return mapToResponse(transaction);
        }

        String otpKey = "verification:otp"+transactionId;

        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if(storedOtp == null){
            log.warn("OTP expired for transaction: {}",transactionId);

            compensateTransaction(transaction,"transaction cancelled and amount refunded.");

            return mapToResponse(transaction);
        }

        if(!storedOtp.equals(otp)){
            log.warn("Wrong OTP entered - blocking account and refunding : {} ",transactionId);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensate(transaction,"Wrong OTP entered - transaction cancelled,"+
                                        "account blocked for security");

            return mapToResponse(transaction);
        }

        //otp correct
        log.info("OTP verified - completing transaction: {}",transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);

        return mapToResponse(transaction);
    }

    private void completeTransaction(Transaction transaction) {
          transaction.setStatus(TransactionStatus.COMPLETED);
          transaction.setCompletedAt(LocalDateTime.now());
          transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getReceiverAccountNumber(),
                transaction.getAmount(),
                transaction.getDescription()

        );

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC,transaction.getId(),completedEvent);

        log.info("SAGA Completed - transaction {} Completed",transaction.getId());
    }

    private void blockAccountAndCompensate(Transaction transaction, String reason) {
        //PUBLISH fraud.detection -> Account Service will block account

        Map<String,Object> fraudEvent = new HashMap<>();

        fraudEvent.put("transactionId",transaction.getId());
        fraudEvent.put("accountNumber",transaction.getSenderAccountNumber());
        fraudEvent.put("amount",transaction.getAmount());
        fraudEvent.put("reason",reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC,transaction.getSenderAccountNumber(),fraudEvent);

        log.warn("fraud.detected published - account : {} will be blocked , kindly contact to your Bank",transaction.getSenderAccountNumber());

        //SAGA COMPENSATION - refund sender
        compensateTransaction(transaction,reason);


    }

    private void compensateTransaction(Transaction transaction, String reason) {
        log.warn("SAGA COMPENSATED - refunding : {} amount : {}", transaction.getSenderAccountNumber(),transaction.getAmount());

        accountServiceClient.creditBalance(
                transaction.getSenderAccountNumber(),
                transaction.getAmount()
        );

        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason + "SAGA Compensation executed, amount refunded at " + LocalDateTime.now());

        transactionRepository.save(transaction);

        //PUBLISH REFUND EVENT - NOTIFICATION SERVICE WILL ALERT USER

        Map<String,Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId",transaction.getId());
        refundEvent.put("senderAccountNumber",transaction.getSenderAccountNumber());
        refundEvent.put("amount",transaction.getAmount());
        refundEvent.put("reason",reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC,transaction.getId(),refundEvent);

        log.info("SAGA COMPENSATION COMPLETED - {} refunded to {}",transaction.getAmount(),transaction.getSenderAccountNumber());
    }


    //SAGA   ------> STEP NO.1 (INITIATE TRANSFER)
    //DEDUCTS FROM THE SENDER VIA FEIGN
    //SAVES TRANSACTION AS AN PROCESSING
    //PUBLISH EVENT TO KAFKA FOR THE FROUD CHECK
    public TransactionResponse transfer(TransactionRequest transactionRequest) {
       log.info("SAGA START- Transfer  {} -> {} amount: {}",transactionRequest.getSenderAccountNumber(),transactionRequest.getReceiverAccountNumber(),transactionRequest.getAmount());

        accountServiceClient.deductBalance(transactionRequest.getSenderAccountNumber(),transactionRequest.getAmount());

        Transaction transaction = new Transaction();

        transaction.setSenderAccountNumber(transactionRequest.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(transactionRequest.getReceiverAccountNumber());
        transaction.setAmount(transactionRequest.getAmount());
        transaction.setType(TrancsactionType.TRANSFER);
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setDescription(transactionRequest.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepository.save(transaction);

        log.info("Transaction Saved as Processing: {}", savedTransaction.getId());


        ///SAGA STEP: 2 Publish for FRAUD CHECK

        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()
        );

      kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC,savedTransaction.getId(), event);

      log.info("SAGA STEP 2:Transaction initiated event published {} :",savedTransaction.getId());

      return mapToResponse(savedTransaction);
    }


    private TransactionResponse mapToResponse(Transaction transaction){
        TransactionResponse transactionResponse = new TransactionResponse();

        transactionResponse.setId(transaction.getId());
        transactionResponse.setAmount(transaction.getAmount());
        transactionResponse.setReferenceNumber(transaction.getReferenceNumber());
        transactionResponse.setType(transaction.getType());
        transactionResponse.setDescription(transaction.getDescription());
        transactionResponse.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        transactionResponse.setSenderAccountNumber(transaction.getSenderAccountNumber());
        transactionResponse.setCreatedAt(transaction.getCreatedAt());
        transactionResponse.setCompletedAt(transaction.getCompletedAt());
        transactionResponse.setFailureReason(transaction.getFailureReason());
        transactionResponse.setStatus(transaction.getStatus());


        return transactionResponse;
    }


    public void processCleanResult(String transactionId) {

        Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(()-> new RuntimeException("Transaction not Found"));

        if(transaction.getStatus() != TransactionStatus.PROCESSING){
            log.warn("Transaction {} not PROCESSING - Skiipping",transactionId);
            return;
        }

        completeTransaction(transaction);
    }
}
