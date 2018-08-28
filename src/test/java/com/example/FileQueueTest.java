package com.example;

import com.example.exception.FileQueueException;
import com.example.helper.FileQueueHelper;
import com.example.model.Message;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class FileQueueTest {
    private static final String queueURL = "queue";
    private Message testMessage1 = new Message("hello1");
    private Message testMessage2 = new Message("hello2");
    private QueueService queueService;
    private FileQueueHelper fileQueueHelper;

    @Before
    public void setup() {
        fileQueueHelper = mock(FileQueueHelper.class);
        queueService = new FileQueueService(fileQueueHelper, Duration.ofSeconds(10));
    }

    @Test
    public void push_shouldCreateQueueIfItNotExists_AndReturnTrue_ifMessageIsSuccessFullyPushedInQueue() throws IOException {
        doReturn(false).when(fileQueueHelper).queueExists(queueURL);

        assertTrue(queueService.push(queueURL, testMessage1));

        verify(fileQueueHelper).takeLock(queueURL);
        verify(fileQueueHelper).createFileQueue(queueURL);
        verify(fileQueueHelper).writeMultipleMessages(queueURL, Lists.newArrayList(testMessage1));
        verify(fileQueueHelper).releaseLock(queueURL);
    }

    @Test
    public void push_shouldAppendMessageToQueue_AndReturnTrue() throws IOException {
        when(fileQueueHelper.queueExists(queueURL)).thenReturn(false).thenReturn(true);

        queueService.push(queueURL, testMessage1);
        assertTrue(queueService.push(queueURL, testMessage2));

        verify(fileQueueHelper).createFileQueue(queueURL);
        verify(fileQueueHelper).writeMultipleMessages(queueURL, Lists.newArrayList(testMessage1));
        verify(fileQueueHelper, atLeast(2)).takeLock(queueURL);
        verify(fileQueueHelper).appendSingleMessage(queueURL, testMessage2);
        verify(fileQueueHelper, atLeast(2)).releaseLock(queueURL);
    }

    @Test(expected = FileQueueException.class)
    public void push_shouldReleaseLock_AndThrow_FileQueueException_ifIOExceptionFound() throws IOException {
        doThrow(new IOException()).when(fileQueueHelper).takeLock(queueURL);

        queueService.push(queueURL, testMessage1);
    }


    @Test
    public void pull_shouldReturn_anyVisibleMessage_fromQueueIfAvailable_AndUpdateVisibilityOfMessage() throws IOException {

        queueService.push(queueURL, testMessage1);

        when(fileQueueHelper.readMessages(queueURL)).thenReturn(Lists.newArrayList(testMessage1));
        Optional<Message> receivedMessage = queueService.pull(queueURL);
        receivedMessage.get().getVisibleFrom().isAfter(LocalDateTime.now());

        assertEquals(testMessage1, receivedMessage.get());

    }

    @Test
    public void pull_shouldReturn_emptyOptional_ifNoVisibleMessageIsAvailable() {
        Optional<Message> receivedMessage = queueService.pull(queueURL);

        assertFalse(receivedMessage.isPresent());
    }

    @Test(expected = FileQueueException.class)
    public void pull_shouldReleaseLock_AndThrow_FileQueueException_ifIOExceptionFound() throws IOException {
        doThrow(new IOException()).when(fileQueueHelper).takeLock(queueURL);

        queueService.pull(queueURL);
    }

    @Test
    public void pull_shouldTryToFollow_FIFO() throws IOException {
        when(fileQueueHelper.queueExists(queueURL)).thenReturn(false).thenReturn(true);
        when(fileQueueHelper.readMessages(queueURL)).thenReturn(Lists.newArrayList(testMessage1, testMessage2)).thenReturn(Lists.newArrayList(testMessage1, testMessage2));

        queueService.push(queueURL, testMessage1);
        queueService.push(queueURL, testMessage2);
        Optional<Message> receivedMessage = queueService.pull(queueURL);
        Optional<Message> receivedMessage2 = queueService.pull(queueURL);

        assertEquals(testMessage1, receivedMessage.get());
        assertEquals(testMessage2, receivedMessage2.get());

        verify(fileQueueHelper).createFileQueue(queueURL);
        verify(fileQueueHelper, times(2)).writeMultipleMessages(queueURL, Lists.newArrayList(testMessage1, testMessage2));
        verify(fileQueueHelper).writeMultipleMessages(queueURL, Lists.newArrayList(testMessage1));
        verify(fileQueueHelper, atLeast(4)).takeLock(queueURL);
        verify(fileQueueHelper).appendSingleMessage(queueURL, testMessage2);
        verify(fileQueueHelper, atLeast(4)).releaseLock(queueURL);
    }


    @Test
    public void delete_shouldReturn_True_IfMessageIsFoundInQueueAndRemoved() throws IOException {
        doReturn(false).when(fileQueueHelper).queueExists(queueURL);
        doReturn(Lists.newArrayList(testMessage1)).when(fileQueueHelper).readMessages(queueURL);

        queueService.push(queueURL, testMessage1);
        assertTrue(queueService.delete(queueURL, testMessage1));

        verify(fileQueueHelper, times(2)).takeLock(queueURL);
        verify(fileQueueHelper).createFileQueue(queueURL);
        verify(fileQueueHelper).writeMultipleMessages(queueURL, Lists.newArrayList(testMessage1));
        verify(fileQueueHelper).writeMultipleMessages(queueURL, Lists.newArrayList());
        verify(fileQueueHelper, times(2)).releaseLock(queueURL);
    }

    @Test
    public void delete_shouldReturn_False_IfMessageIsNotFoundInQueue() {
        assertFalse(queueService.delete(queueURL, testMessage1));
    }

    @Test(expected = FileQueueException.class)
    public void delete_shouldReleaseLock_AndThrow_FileQueueException_ifIOExceptionFound() throws IOException {
        doThrow(new IOException()).when(fileQueueHelper).readMessages(queueURL);

        queueService.delete(queueURL, testMessage1);

        verify(fileQueueHelper).takeLock(queueURL);
        verify(fileQueueHelper).releaseLock(queueURL);
    }
}
