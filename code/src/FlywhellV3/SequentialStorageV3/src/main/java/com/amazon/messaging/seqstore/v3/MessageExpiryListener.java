package com.amazon.messaging.seqstore.v3;

public interface MessageExpiryListener {

    public void notifyMessagesExpired(String destination, String reader, long number);
}
