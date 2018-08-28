package com.example;

import com.example.exception.FileQueueException;
import com.example.helper.FileQueueHelper;
import com.example.model.Message;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * A queue service using File as storage. Supports many producers and many consumers.
 * <p>
 * Achieve inter-process safety using uses a .lock folder per queue
 */
public class FileQueueService implements QueueService {
    private FileQueueHelper fileQueueHelper;
    private Duration inVisibilityDuration;


    protected FileQueueService(FileQueueHelper fileQueueHelper, Duration inVisibilityDuration) {
        this.fileQueueHelper = fileQueueHelper;
        this.inVisibilityDuration = inVisibilityDuration;
    }

    @Override
    public Boolean push(String queueURL, Message message) {
        QueueService.validateQueueURL(queueURL);
        QueueService.validateMessage(message);
        try {
            fileQueueHelper.takeLock(queueURL);
            // check if file queue exists
            if (fileQueueHelper.queueExists(queueURL)) {
                fileQueueHelper.appendSingleMessage(queueURL, message);
            } else {
                // create file queue
                fileQueueHelper.createFileQueue(queueURL);
                List<Message> messages = Lists.newArrayList(message);
                fileQueueHelper.writeMultipleMessages(queueURL, messages);
            }
            return true;
        } catch (IOException e) {
            throw new FileQueueException("Error while pushing message: {" + message + "} to queue: " + queueURL, e);
        } finally {
            fileQueueHelper.releaseLock(queueURL);
        }
    }

    @Override
    public Optional<Message> pull(String queueURL) {
        QueueService.validateQueueURL(queueURL);
        Optional<Message> message;

        try {
            fileQueueHelper.takeLock(queueURL);
            // read messages in a queue
            List<Message> messages = fileQueueHelper.readMessages(queueURL);
            // find first visible message
            message = QueueService.findVisibleMessage(messages);
            if (message.isPresent()) {
                Message msg = message.get();
                // set visibility to be turned off until current time + duration provided in inVisibilityDuration
                msg.setVisibleFrom(LocalDateTime.now().plus(inVisibilityDuration));
                // write updated message to file queue
                fileQueueHelper.writeMultipleMessages(queueURL, messages);
            }

        } catch (IOException e) {
            throw new FileQueueException("Error while getting message from queue: " + queueURL, e);
        } finally {
            fileQueueHelper.releaseLock(queueURL);
        }

        return message;

    }

    @Override
    public Boolean delete(String queueURL, Message message) {
        QueueService.validateQueueURL(queueURL);
        QueueService.validateMessage(message);
        QueueService.validateReceiptHandle(message.getReceiptHandle());
        boolean result;

        try {
            fileQueueHelper.takeLock(queueURL);
            List<Message> messages = fileQueueHelper.readMessages(queueURL);
            // remove message from file queue if present
            result = messages.removeIf(msg -> msg.getReceiptHandle().equals(message.getReceiptHandle()));
            // update file queue
            fileQueueHelper.writeMultipleMessages(queueURL, messages);
        } catch (IOException e) {
            throw new FileQueueException("Error while deleting message: {" + message + "} to queue: " + queueURL, e);
        } finally {
            fileQueueHelper.releaseLock(queueURL);
        }
        return result;
    }
}



