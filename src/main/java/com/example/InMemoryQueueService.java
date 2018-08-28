package com.example;

import com.example.model.Message;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A queue service using in-memory storage. Supports many producers and many consumers.
 * <p>
 * This in-memory queue is based on :
 * - ConcurrentLinkedQueue: concurrent FIFO queue to store the messages with a ConcurrentHashMap.
 * - ConcurrentHashMap: to provide bucket-locking per queue
 */
public class InMemoryQueueService implements QueueService {
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Message>> queues = new ConcurrentHashMap<>();

    private Duration inVisibilityDuration;

    protected InMemoryQueueService(Duration inVisibilityDuration) {
        this.inVisibilityDuration = inVisibilityDuration;
    }

    @Override
    public Boolean push(String queueURL, Message message) {
        QueueService.validateQueueURL(queueURL);
        QueueService.validateMessage(message);
        ConcurrentLinkedQueue<Message> queue = queues.get(queueURL);
        if (queue == null) {
            // create queue if it doesn't exist
            queue = new ConcurrentLinkedQueue();
            queues.put(queueURL, queue);
        }
        return queue.offer(message);
    }

    @Override
    public Optional<Message> pull(String queueURL) {
        QueueService.validateQueueURL(queueURL);
        ConcurrentLinkedQueue<Message> queue = queues.get(queueURL);
        if (queue != null) {
            // find first visibile message
            Optional<Message> message = QueueService.findVisibleMessage(queue);
            // set invisibility period
            message.ifPresent(msg -> msg.setVisibleFrom(LocalDateTime.now().plus(inVisibilityDuration)));
            return message;
        } else
            return Optional.empty();
    }

    @Override
    public Boolean delete(String queueURL, Message message) {
        QueueService.validateQueueURL(queueURL);
        QueueService.validateMessage(message);
        QueueService.validateReceiptHandle(message.getReceiptHandle());

        ConcurrentLinkedQueue<Message> queue = queues.get(queueURL);

        if (queue != null) {
            // delete message from queue, if present
            Boolean deleteResult = queue.removeIf(msg -> msg.getReceiptHandle().equals(message.getReceiptHandle()));
            return deleteResult;
        } else
            return false;
    }


}
