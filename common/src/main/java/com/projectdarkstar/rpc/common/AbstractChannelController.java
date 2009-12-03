package com.projectdarkstar.rpc.common;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.projectdarkstar.rpc.CoreRpc.*;

import java.nio.ByteBuffer;

public abstract class AbstractChannelController implements ChannelController {

    public void sendResponse(final int requestId, final Message message) {
        final Header.Builder headerBuilder = Header.newBuilder();
        headerBuilder.setMessageType(Header.MessageType.RESPONSE);
        headerBuilder.setRequestId(requestId);
        sendMessage(headerBuilder.build(), message);
    }

    @Override
    public void sendMessage(Header header, Message message) {
        final byte[] headerBytes = header.toByteArray();
        final byte[] messageBytes = message.toByteArray();

        ByteBuffer byteBuffer = ByteBuffer.allocate(1 + headerBytes.length + messageBytes.length);
        byteBuffer.put((byte) headerBytes.length);
        byteBuffer.put(headerBytes);
        byteBuffer.put(messageBytes);
        byteBuffer.flip();

        sendToChannel(byteBuffer);
    }

    @Override
    public void receivedMessage(ByteBuffer message, LocalRegistry local) {
        final int headerLength = message.get();
        final byte[] header = new byte[headerLength];

        if (message.remaining() < headerLength) {
            // TODO log
            return;
        }

        // Load the header.
        message.get(header);

        final Header messageHeader;
        try {
            messageHeader = Header.newBuilder().mergeFrom(header).build();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Unable to parse message header", e);
        }

        switch (messageHeader.getMessageType()) {
            case ASYNC_REQUEST:
                break;
            case SYNC_REQUEST:
                local.receivedRequest(messageHeader, message);
                break;
            case RESPONSE:
                receivedResponse(messageHeader, message);
                break;
            default:
                //TODO log
                break;
        }
    }

    public void receivedResponse(Header header, ByteBuffer byteBuffer) {
        int requestId = header.getRequestId();
        final RemoteCall remoteCall = getCallbackCache().removeCallback(requestId);
        final RpcCallback<Message> callback = remoteCall.getCallback();
        final Message prototype = remoteCall.getResponsePrototype();

        callback.run(ByteBufferInputStream.buildMessage(prototype, byteBuffer));
    }

    protected abstract CallbackCache getCallbackCache();

    protected abstract void sendToChannel(ByteBuffer buf);
}
