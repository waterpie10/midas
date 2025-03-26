package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    @Value("${general.kafka-topic}")
    private String topic;

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

    public TransactionListener(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-consumer-group")
    public void consumeTransaction(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

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

        // Adjust the balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // Persist the updated user records
        userRepository.save(sender);
        userRepository.save(recipient);

        // Record the transaction
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
        transactionRecordRepository.save(record);

        logger.info("Transaction processed and recorded: {}", record);
    }
}
