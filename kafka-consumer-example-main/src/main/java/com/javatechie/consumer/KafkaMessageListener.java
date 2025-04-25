package com.javatechie.consumer;

import com.javatechie.dto.Customer;
import com.javatechie.repository.CustomerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {

    @Autowired
    private CustomerRepository customerRepository;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    @KafkaListener(topics = "javatechie-demo",groupId = "jt-group")
    public void consumeEvents(Customer customer) {
        log.info("consumer consume the events {} ", customer.toString());

        //DB Operation -- MYSQL save
        customerRepository.save(customer);
        System.out.println("Customer Saved Successfully");

        String responseMessage = "Customer with ID " + customer.getId() + " saved successfully.";
        kafkaTemplate.send("javatechie-demo-1", responseMessage);
        log.info("Sent confirmation message to producer: {}", responseMessage);
    }
}
