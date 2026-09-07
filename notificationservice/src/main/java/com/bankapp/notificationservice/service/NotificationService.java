package com.bankapp.notificationservice.service;


import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class NotificationService {

    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOtpGenerated(
            @Payload Map<String,Object> payload
            ) {
        try {
            String accountNumber = payload.get("accountNumber").toString();
            String otp = payload.get("otp").toString();
            String transactionId = payload.get("transactionId").toString();
            String amount = payload.get("amount").toString();
            String reason = payload.get("reason").toString();

            sendAlert(accountNumber,
                    "TRANSACTION VERIFICATION REQUIRED",
                    String.format(
                            "Suspicious activity detected on your account" +
                                    "Reason: %s " +
                                    "A transaction of %s is pending verification." +
                                    "Your OTP is : %s . Valid till 5 minutes." +
                                    "If this wasn't you - ignore this message.",reason,amount,otp
                    )
            );
    }catch(Exception e){
               log.error("Error sending OTP Notification : {}",e.getMessage());
          }
    }


    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String,Object> payload){
        try{
            String senderAccount = (String) payload.get("senderAccountNumber");
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            String amount = payload.get("amount").toString();


            ////Debit Alert
            sendAlert(senderAccount,
                    "Debit Alert",
                       String.format(
                               "%s debited from account %s",
                               amount,senderAccount
                       )
                    );

           //////Credit Alert
            sendAlert(receiverAccount,
                    "Credit Alert",
                    String.format(
                            "%s credited to account %s",
                            amount,receiverAccount
                    )
            );

        }catch(Exception e){
             log.error("Error Sending transaction notification : {}",e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.detection")
    public void consumeFraudDetected(@Payload Map<String,Object> payload){

            try{
                String accountNumber = (String) payload.get("accountNumber");
                String reason = (String) payload.get("reason");

                sendAlert(accountNumber,
                        "SUSPICIOUS ACTIVITY DETECTED",
                        String.format(
                            "Your account %s has been blocked ."+
                            "Reason : %s ."+
                            "Please contact your bank Immediately.",
                             accountNumber,reason
                        ));


            }catch (Exception e){
                log.error("Error Sending fraud alert : {}",e.getMessage());
            }

    }



    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefunded(@Payload Map<String,Object> payload){
          try{

              String senderAccount = (String) payload.get("senderAccountNumber");
              String amount = payload.get("amount").toString();
              String reason = (String) payload.get("reason");

              sendAlert(
                      senderAccount,
                      "REFUND PROCESSED",
                      String.format(
                              "Your transaction of %s was cancelled and %s was cancelled."+
                                "Reason : %s ."+
                              "%s has been refunded to account %s."
                        ,amount,reason,amount,senderAccount
                      )
              );

          }catch(Exception e){
               log.error("Error Sending Notification {} - Refund",e.getMessage());
          }
    }


    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(@Payload Map<String,Object> payload){
         try{
             String accountNumber = (String) payload.get("accountNumber");
             String amount = payload.get("amount").toString();

             sendAlert(accountNumber,
                     "PAYMENT SUCCESSFUL",
                     String.format(
                             "Payment of %s is completed."+
                             "Razorpay ID : %s",amount,payload.get("razorpayPaymentId")
                     ));

         }catch(Exception e){
             log.error("Error Sending Successful payment alert : {}",e.getMessage());
         }

    }


    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(@Payload Map<String,Object> payload){
        try{
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(accountNumber,
                    "PAYMENT Failed",
                    String.format(
                            "Payment of %s is failed."+
                             "Please try Again or Contact support",amount
                    ));

        }catch(Exception e){
            log.error("Error Sending Failed payment alert : {}",e.getMessage());
        }

    }


    private void sendAlert(String accountNumber,String subject,String message){
       log.info("-----------------------------------------------");
       log.info("Account: {}",accountNumber);
       log.info("Subject: {}",subject);
       log.info("Message: {}",message);
       log.info("------------------------------------------------");


    }

}
