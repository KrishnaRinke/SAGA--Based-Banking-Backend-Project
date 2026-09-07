package com.bankapp.paymentservice.service;

import com.bankapp.paymentservice.dto.CreatePaymentRequest;
import com.bankapp.paymentservice.dto.PaymentOrderResponse;
import com.bankapp.paymentservice.entity.Payment;
import com.bankapp.paymentservice.entity.PaymentStatus;
import com.bankapp.paymentservice.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.UUID;
import org.json.JSONObject;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;

    private final KafkaTemplate<String,Object> kafkaTemplate;

    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.key-secret}")
    private String keySecret;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";


    /**
     * Create Razorpay payment order
     *
     * flow
     * 1.Create order in razorpay
     * 2.save Payment record in DB
     * 3.Return order details
     * 4.Frontend show Razorpay Checkout
     * 5.UserPays
     * 6.Razorpay calls webhooks
     * @param createPaymentRequest
     * @return
     */


    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest createPaymentRequest) throws RazorpayException {
       log.info("Creating payment order for the account : {} amount : {}",createPaymentRequest.getAccountNumber(),createPaymentRequest.getAmount());

        RazorpayClient razorpayClient = new RazorpayClient(keyId,keySecret);

        //Converted to lower currency

        int convertedAmount = createPaymentRequest.getAmount().multiply(BigDecimal.valueOf(100)).intValue();

//        JSONObject orderRequest = new JSONObject();
//
//        ///ye exact pattern chaiye hota hai razorpay ko for the order Id detail ke liye
//        orderRequest.put("amount",convertedAmount);
//        orderRequest.put("currency","INR");
//        orderRequest.put("receipt","receipt_"+ System.currentTimeMillis() + UUID.randomUUID().toString()
//                .replace("-","").substring(0,10));
//
//        Order razorpayOrder = razorpayClient.orders.create(orderRequest);

        JSONObject orderRequest = new JSONObject();

        orderRequest.put("amount", convertedAmount);
        orderRequest.put("currency", "INR");
        orderRequest.put("receipt", "receipt_" + System.currentTimeMillis());

        log.info("Razorpay Order Request: {}", orderRequest);

        Order razorpayOrder = razorpayClient.orders.create(orderRequest);

        log.info("Razorpay order created : {}",razorpayOrder.get("id").toString());

        //save Payment record
        Payment payment = new Payment();
        payment.setRazorpayOrderId(razorpayOrder.get("id").toString());
        payment.setAccountNumber(createPaymentRequest.getAccountNumber());
        payment.setAmount(createPaymentRequest.getAmount());
        payment.setCurrency("INR");
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(createPaymentRequest.getDescription());

       Payment savedPayment = paymentRepository.save(payment);

       return new PaymentOrderResponse(
             savedPayment.getId(),
             razorpayOrder.get("id").toString(),
             savedPayment.getAmount(),
             "INR",
             "CREATED",
             keyId
       );

    }


    //mechanism to tell is the payment successful of failed to backend
    public void handleWebhook(Map<String, Object> payload) {

     log.info("Received Razorpay webhook: {}",payload.get("event"));

     String event = (String) payload.get("event");

     if("payment.captured".equals(event)){
         handlePaymentSuccess(payload);
     }
     else if("payment.failed".equals(event)){
         handlePaymentFailed(payload);
     }


    }

    private void handlePaymentFailed(Map<String, Object> payload) {
        try{
            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId).orElseThrow(()->new RuntimeException("Payment not Found for  Order RazorPay"));

            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment Failed via RazorPay");
            paymentRepository.save(payment);

            Map<String,Object> event = new HashMap<>();

            event.put("paymentId",payment.getId());
            event.put("accountNumber",payment.getAccountNumber());
            event.put("amount",payment.getAmount());
            event.put("reason","Payment Failed via RazorPay");

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC,payment.getId(),event);
            log.warn("Payment Failed: {}",payment.getId());

        }catch(Exception e){

            log.error("error in Failed Razorpay payment",e.getMessage());

        }
    }

    private Map<String, Object> extractPaymentData(Map<String, Object> payload) {
        Map<String,Object> entity = (Map<String,Object>) payload.get("payload");

        Map<String,Object> paymentWrapper = (Map<String,Object>) entity.get("payment");

        return (Map<String,Object>) paymentWrapper.get("entity");
    }


    private void handlePaymentSuccess(Map<String, Object> payload) {
        try{
             Map<String,Object> paymentData = extractPaymentData(payload);
             String orderId = (String) paymentData.get("order_id");
             String paymentId = (String) paymentData.get("id");

             Payment payment = paymentRepository.findByRazorpayOrderId(orderId).orElseThrow(()->new RuntimeException("Payment not Found for  Order RazorPay"));

             payment.setRazorpayPaymentId(paymentId);
             payment.setStatus(PaymentStatus.COMPLETED);
             paymentRepository.save(payment);

             //Publish payment complete event

            Map<String,Object> event = new HashMap<>();

            event.put("paymentId",payment.getId());
            event.put("accountNumber",payment.getAccountNumber());
            event.put("amount",payment.getAmount());
            event.put("razorpayPaymentId",paymentId);

           kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC,payment.getId(),event);
           log.info("Payment Completed: {}",payment.getId());

        }catch(Exception e){

            log.error("Error handling payment success : {}",e.getMessage());
        }

    }


}
