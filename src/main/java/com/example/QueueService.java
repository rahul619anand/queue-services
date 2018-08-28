package com.example;

import com.example.model.Message;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import java.util.NoSuchElementException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Contract for a generic queue service
 */
public interface QueueService {
    // Validation constants
    String INVALID_MESSAGE = "message must not be null";
    String INVALID_QUEUE_URL = "queueUrl must not be null or empty";
    String INVALID_RECEIPT_HANDLE = "receipt handle must not be null or empty";

    /**
     * Pushes a message onto a queue.
     *
     * @param queueURL queueURL or Name
     * @param message  message to be pushed to queue
     * @return boolean as successful or failed push of a message to queue
     */
    Boolean push(String queueURL, Message message);

    /**
     * Retrieves a single message from a queue.
     *
     * @param queueURL queueURL or Name
     * @return first visible message in Optional if available, else Optional.Empty()
     */
    Optional<Message> pull(String queueURL);

    /**
     * Deletes a message from the queue that was received by pull().
     *
     * @param queueURL queueURL or Name
     * @param message  message to be deleted from queue
     * @return boolean as successful or failed delete of a message from queue
     */
    Boolean delete(String queueURL, Message message);

    /**
     * Validates that message is not null
     *
     * @param message Message
     * @throws IllegalArgumentException with relevant message, if invalid message
     */
    static void validateMessage(Message message) {
        checkArgument(message != null, INVALID_MESSAGE);
    }

    /**
     * Validates that queueUrl is not null or empty
     *
     * @param queueUrl queueURL or Name
     * @throws IllegalArgumentException with relevant message, if invalid queueUrl
     */
    static void validateQueueURL(String queueUrl) {
        checkArgument(!Strings.isNullOrEmpty(queueUrl), INVALID_QUEUE_URL);
    }

    /**
     * Validates that receiptHandle is not null or empty
     *
     * @param receiptHandle receiptHandle of the Message
     * @throws IllegalArgumentException with relevant message, if invalid receiptHandle
     */
    static void validateReceiptHandle(String receiptHandle) {
        checkArgument(!Strings.isNullOrEmpty(receiptHandle), INVALID_RECEIPT_HANDLE);
    }

    /**
     * Finds first visible message in all the messages of a queue
     *
     * @param messages messages of a queue
     * @return first visible message in Optional if available, else Optional.Empty()
     */
    static Optional<Message> findVisibleMessage(Iterable<Message> messages) {
        try {
            Message message = Iterables.find(messages, msg -> msg.isVisible());
            return Optional.of(message);
        } catch (NoSuchElementException e) {
            return Optional.empty();
        }
    }
}

//* Can make following improvements to the solution:
//        *
//        * Feature Wise :
//        * 1. Can include induced delay while pushing a message, feature for queue max limit etc.
//        *
//        * Code Cleanup and Design:
//        * 1. remove a bit of redundant code here and there
//        * 2. provide an abstraction layer for QueueService
//        * 3. logs and more comments
//        * 4. better exception handling flow
//        * 5. better Test organization and fixtures