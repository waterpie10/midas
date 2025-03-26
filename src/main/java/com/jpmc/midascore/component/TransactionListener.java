package com.jpmc.midascore.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    @Value("${general.kafka-topic}")
    private String topic;

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TransactionListener(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-consumer-group")
    public void consumeTransaction(ConsumerRecord<String, String> record) {
        // Log the raw message payload
        String rawMessage = record.value();
        logger.info("Raw message received from topic {}: {}", record.topic(), rawMessage);

        Transaction transaction = null;
        try {
            // Attempt to convert the raw JSON string to a Transaction object
            transaction = objectMapper.readValue(rawMessage, Transaction.class);
            logger.info("Converted Transaction: {}", transaction);
        } catch (Exception e) {
            logger.error("Error converting message to Transaction", e);
            return;
        }

        // Proceed with processing only if conversion is successful
        Optional<UserRecord> senderOpt = userRepository.findById(transaction.getSenderId());
        Optional<UserRecord> recipientOpt = userRepository.findById(transaction.getRecipientId());

        if (senderOpt.isEmpty() || recipientOpt.isEmpty()) {
            logger.warn("Transaction discarded: Invalid sender or recipient.");
            return;
        }

        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();

        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Transaction discarded: Insufficient funds for sender {}.", sender.getName());
            return;
        }

        // Call the Incentive API
        RestTemplate restTemplate = new RestTemplate();
        Incentive incentiveObj = restTemplate.postForObject("http://localhost:8080/incentive", transaction, Incentive.class);
        float incentiveAmount = (incentiveObj != null) ? incentiveObj.getAmount() : 0;

        // Adjust balances: subtract only the transaction amount from sender;
        // add the transaction amount plus incentive to the recipient.
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        // Save updated user records
        userRepository.save(sender);
        userRepository.save(recipient);

        // Log wilbur's balance if this recipient is wilbur
        if ("wilbur".equalsIgnoreCase(recipient.getName())) {
            logger.info("Wilbur's updated balance: {}", recipient.getBalance());
        }

        // Record the transaction including the incentive
        TransactionRecord recordEntity = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
        transactionRecordRepository.save(recordEntity);
        logger.info("Transaction processed and recorded: {}", recordEntity);
    }
}
