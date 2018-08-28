package com.example.helper;

import com.example.model.Message;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.google.common.primitives.Longs;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.google.common.io.Files.touch;

public class FileQueueHelper {
    private static final Duration FILE_LOCK_TRY_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration FILE_LOCK_RETRY_PERIOD = Duration.ofMillis(50);
    private static final Charset CHAR_SET = StandardCharsets.UTF_8;
    private String parentDirectory;

    FileQueueHelper(String parentDirectory) {
        this.parentDirectory = parentDirectory;
    }

    /**
     * Gets messages in a file queue
     */
    public List<Message> readMessages(String queueName) throws IOException {
        return Files.readLines(getMessagesFile(queueName), CHAR_SET, lineProcessor());
    }

    /**
     * Creates file to be considered as queue, within its parent directory
     */
    public void createFileQueue(String queueName) throws IOException {
        File messagesFile = getMessagesFile(queueName);
        Files.createParentDirs(messagesFile);
        touch(messagesFile);
    }

    /**
     * Checks existence of a file queue
     */
    public boolean queueExists(String queueName) {
        return getMessagesFile(queueName).exists();
    }

    /**
     * Appends a message to a file queue
     */
    public void appendSingleMessage(String queueName, Message message) throws IOException {
        String text = '\n' + toText(message);
        java.nio.file.Files.write(getMessagesFile(queueName).toPath(), text.getBytes(), StandardOpenOption.APPEND);
    }

    /**
     * Appends messages to a file queue
     */
    public void writeMultipleMessages(String queueName, List<Message> messages) throws IOException {
        String messagesToWrite = Joiner.on('\n').join(messages.stream().map(msg -> toText(msg)).collect(Collectors.toList()));
        Files.write(messagesToWrite, getMessagesFile(queueName), CHAR_SET);
    }

    /**
     * Requests lock for accessing a file queue
     */
    public void takeLock(String queueName) throws IOException {
        File lock = getLockFile(queueName);
        Files.createParentDirs(lock);
        withinDuration(tryLock(lock));
    }

    /**
     * Releases lock of file queue
     */
    public void releaseLock(String queueName) {
        File lock = getLockFile(queueName);
        lock.delete();
    }

    /**
     * Gets lock file by queue
     */
    public File getLockFile(String queueName) {
        String pathName = Joiner.on('/').skipNulls().join(parentDirectory, queueName, ".lock");
        return new File(pathName);
    }

    /**
     * Gets actual file queue
     */
    public File getMessagesFile(String queueName) {
        String pathName = Joiner.on('/').skipNulls().join(parentDirectory, queueName, "messages");
        return new File(pathName);
    }

    /**
     * Tries to get lock for accessing a file queue
     */
    private Future tryLock(File lock) {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        Future future = executor.submit((Callable) () -> {
            while (!lock.mkdir()) {

                Thread.sleep(FILE_LOCK_RETRY_PERIOD.toMillis());
            }
            return null;
        });

        return future;
    }

    private void withinDuration(Future future) {
        try {
            future.get(FILE_LOCK_TRY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            future.cancel(true);
        }
    }

    /**
     * LineProcessor for parsing text messages
     */
    private LineProcessor<List<Message>> lineProcessor() {
        return new LineProcessor<List<Message>>() {
            final List<Message> messages = Lists.newArrayList();

            @Override
            public boolean processLine(String line) {
                Message message = toMessage(line);
                messages.add(message);
                return true;
            }

            @Override
            public List<Message> getResult() {
                return messages;
            }
        };
    }

    /**
     * Converts text to a Message object
     */
    public static Message toMessage(String text) {
        List<String> fields = Lists.newArrayList(Splitter.on(":").split(text));
        // converts epoch to LocalDateTime
        LocalDateTime visibleFrom = LocalDateTime.ofInstant(Instant.ofEpochMilli(Longs.tryParse(fields.get(0))), ZoneId.systemDefault());
        String receiptHandle = fields.get(1);
        String messageId = fields.get(2);
        String messageContent = fields.get(3);

        Message message = new Message(receiptHandle, messageId, messageContent, visibleFrom);
        return message;
    }

    /**
     * Converts Message object to text
     */
    public String toText(Message message) {
        // converts LocalDateTime to epoch
        Long time = message.getVisibleFrom().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        return Joiner.on(":").skipNulls().join(time.toString(), message.getReceiptHandle(), message.getId(), message.getContent());
    }
}
