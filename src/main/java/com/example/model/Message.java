package com.example.model;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

/**
 * Generic schema for Message which is eventually queued
 */
public class Message {
    private String id;
    private String receiptHandle;
    private String content;

    /**
     * Field to know when a message is visible.
     * The whole invisibilityTimeout concept works with this field.
     * Every time a message is pulled from queue, this is set to current LocalDateTime + invisibilityDuration
     * By calling isVisible() (which compares current_time with `visibleFrom`),
     * we know if this message should be visible in the queue or not still.
     */
    private LocalDateTime visibleFrom;

    public Message(String content) {
        this.id = UUID.randomUUID().toString();
        this.receiptHandle = UUID.randomUUID().toString();
        this.content = content;
        this.visibleFrom = LocalDateTime.now();
    }

    public Message(String id, String receiptHandle, String content, LocalDateTime visibleFrom) {
        this.id = id;
        this.receiptHandle = receiptHandle;
        this.content = content;
        this.visibleFrom = visibleFrom;
    }

    public String getId() {
        return id;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }

    public String getContent() {
        return content;
    }

    /**
     * checks if this message is visible in queue
     */
    public boolean isVisible() {
        return LocalDateTime.now().isEqual(visibleFrom) || LocalDateTime.now().isAfter(visibleFrom);
    }

    public LocalDateTime getVisibleFrom() {
        return visibleFrom;
    }

    public void setVisibleFrom(LocalDateTime visibleFrom) {
        this.visibleFrom = visibleFrom;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return
                Objects.equals(id, message.id) &&
                        Objects.equals(receiptHandle, message.receiptHandle) &&
                        Objects.equals(content, message.content) &&
                        Objects.equals(visibleFrom, message.visibleFrom);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, receiptHandle, content, visibleFrom);
    }

    @Override
    public String toString() {
        return "Message{" +
                "id='" + id + '\'' +
                ", receiptHandle='" + receiptHandle + '\'' +
                ", content='" + content + '\'' +
                ", visibleFrom=" + visibleFrom +
                '}';
    }
}
