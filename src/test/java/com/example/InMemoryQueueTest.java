package com.example;

import com.example.model.Message;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class InMemoryQueueTest {
    private static final String queueURL = "queue";
    Message testMessage1 = new Message("hello1");
    Message testMessage2 = new Message("hello2");
    private QueueService queueService;

    @Before
    public void setup() {
        Duration invisibilityDuration = Duration.ofSeconds(5);
        queueService = new InMemoryQueueService(invisibilityDuration);
    }

    @Test
    public void push_shouldCreateQueueIfItNotExists_AndReturnTrue_ifMessageIsSuccessFullyPushedInQueue() {
        assertTrue(queueService.push(queueURL, testMessage1));
    }

    @Test
    public void push_shouldAppendMessageToQueue_AndReturnTrue() {
        queueService.push(queueURL, testMessage1);
        assertTrue(queueService.push(queueURL, testMessage1));
    }

    @Test
    public void pull_shouldReturn_anyVisibleMessage_fromQueueIfAvailable() {
        queueService.push(queueURL, testMessage1);
        Optional<Message> receivedMessage = queueService.pull(queueURL);

        assertEquals(testMessage1, receivedMessage.get());
    }

    @Test
    public void pull_shouldReturn_emptyOptional_ifNoVisibleMessageIsAvailable() {
        Optional<Message> receivedMessage = queueService.pull(queueURL);

        assertFalse(receivedMessage.isPresent());
    }

    @Test
    public void pull_shouldReturn_message_ifAnInvisibleMessageBecomeVisibleAfterCertainDuration() {
        queueService.push(queueURL, testMessage1);
        Optional<Message> message = queueService.pull(queueURL);
        message.ifPresent(m -> m.setVisibleFrom(LocalDateTime.now()));

        Optional<Message> receivedMessage = queueService.pull(queueURL);

        assertEquals(testMessage1, receivedMessage.get());
    }

    @Test
    public void pull_shouldReturn_nextVisibleMessage_ifFirstMessageIsInVisible() {
        queueService.push(queueURL, testMessage1);
        queueService.push(queueURL, testMessage2);
        queueService.pull(queueURL);
        Optional<Message> receivedMessage = queueService.pull(queueURL);
        assertEquals(testMessage2, receivedMessage.get());
    }


    @Test
    public void pull_shouldTryToFollow_FIFO() {
        queueService.push(queueURL, testMessage1);
        queueService.push(queueURL, testMessage2);
        Optional<Message> receivedMessage = queueService.pull(queueURL);
        Optional<Message> receivedMessage2 = queueService.pull(queueURL);
        assertEquals(testMessage1, receivedMessage.get());
        assertEquals(testMessage2, receivedMessage2.get());
    }

    @Test
    public void pull_shouldSet_messageInVisibilityDuration() {
        queueService.push(queueURL, testMessage1);
        Optional<Message> receivedMessage = queueService.pull(queueURL);
        assertTrue(receivedMessage.get().getVisibleFrom().isAfter(LocalDateTime.now()));
    }

    @Test
    public void delete_shouldReturn_True_IfMessageIsFoundInQueueAndRemoved() {
        queueService.push(queueURL, testMessage1);
        assertTrue(queueService.delete(queueURL, testMessage1));

    }

    @Test
    public void delete_shouldReturn_False_IfMessageIsNotFoundInQueue() {
        assertFalse(queueService.delete(queueURL, testMessage1));
    }

}
