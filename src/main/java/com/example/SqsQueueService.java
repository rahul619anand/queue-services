package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Wrapper around Amazon's Simple Queue Service
 */
public class SqsQueueService implements QueueService {

    private AmazonSQSClient sqsClient;

    // this is default sqs visibility timeout
    private Duration inVisibilityDuration = Duration.ofSeconds(30);

    public SqsQueueService(AmazonSQSClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    public SqsQueueService(AmazonSQSClient sqsClient, Duration inVisibilityDuration) {
        this.sqsClient = sqsClient;
        this.inVisibilityDuration = inVisibilityDuration;
    }


    @Override
    public Boolean push(String queueURL, com.example.model.Message message) {
        SendMessageRequest sendMessageRequest = new SendMessageRequest(queueURL, message.getContent());
        sqsClient.sendMessage(sendMessageRequest);
        return true;
    }

    @Override
    public Optional<com.example.model.Message> pull(String queueURL) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueURL);
        ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
        Optional<com.amazonaws.services.sqs.model.Message> sqsMessage = receiveMessageResult.getMessages().stream().findFirst();
        Optional<com.example.model.Message> receivedMessage = sqsMessage.map(msg ->
                // set inVisibilityDuration on the message
                new com.example.model.Message(msg.getMessageId(), msg.getReceiptHandle(), msg.getBody(), LocalDateTime.now().plus(inVisibilityDuration))
        );
        return receivedMessage;
    }

    @Override
    public Boolean delete(String queueURL, com.example.model.Message message) {
        DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(queueURL, message.getReceiptHandle());
        sqsClient.deleteMessage(deleteMessageRequest);
        return true;
    }
}
