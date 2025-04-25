package com.javatechie.service;

import com.javatechie.dto.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String,Object> template;

    private static final Logger log = LoggerFactory.getLogger(KafkaMessagePublisher.class);

    @KafkaListener(topics = "javatechie-demo-1", groupId = "producer-response-group")
    public void consumeResponse(String responseMessage) {
        log.info("Producer received response message: {}", responseMessage);
        System.out.println(System.currentTimeMillis());
    }

    public void sendMessageToTopic(String message){
        CompletableFuture<SendResult<String, Object>> future = template.send("javatechie-demo", message);
        future.whenComplete((result,ex)->{
            if (ex == null) {
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        message + "] due to : " + ex.getMessage());
            }
        });

    }

    public void sendEventsToTopic(Customer customer) {
        try {
            CompletableFuture<SendResult<String, Object>> future = template.send("javatechie-demo", customer);
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Sent message=[" + customer.toString() +
                            "] with offset=[" + result.getRecordMetadata().offset() + "]");
                } else {
                    System.out.println("Unable to send message=[" +
                            customer.toString() + "] due to : " + ex.getMessage());
                }
            });

        } catch (Exception ex) {
            System.out.println("ERROR : "+ ex.getMessage());
        }
    }

    public void sendMsg()
    {
        for(int i=0;i<1000;i++)
        {
            Customer cust = new Customer(i,"temp","temp","000");
            sendEventsToTopic(cust);
        }
    }
}
