package com.projectdarkstar.rpc.common;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;

import java.nio.ByteBuffer;

public abstract class AbstractChannelController implements ChannelController {

    @Override
    public void sendRequest(int serviceId, int methodId, long requestId, Message requestParameter) {
        final byte[] bytes = requestParameter.toByteArray();

        ByteBuffer buf = ByteBuffer.allocate(bytes.length + 10);
        buf.put((byte) serviceId);
        buf.put((byte) methodId);
        sendToChannel(requestId, bytes, buf);
    }

    @Override
    public void sendResponse(int serviceId, long requestId, Message response) {
        final byte[] bytes = response.toByteArray();

        ByteBuffer buf = ByteBuffer.allocate(bytes.length + 9);
        buf.put((byte) (serviceId - 128));
        sendToChannel(requestId, bytes, buf);
    }

    private void sendToChannel(long requestId, byte[] bytes, ByteBuffer buf) {
        buf.putLong(requestId);
        buf.put(bytes);
        buf.flip();
        sendToChannel(buf);
    }

    public void receivedMessage(ByteBuffer message, LocalRegistry local) {
        int type = message.get();
        if (type >= 0) {
            local.receivedRequest(type, message);
        } else {
            receivedResponse(message);
        }
    }

    public void receivedResponse(ByteBuffer byteBuffer) {
        long requestId = byteBuffer.getLong();
        final RemoteCall remoteCall = getCallbackCache().removeCallback(requestId);
        final RpcCallback<Message> callback = remoteCall.getCallback();
        final Message prototype = remoteCall.getResponsePrototype();

        callback.run(ByteBufferInputStream.buildMessage(prototype, byteBuffer));
    }

    protected abstract CallbackCache getCallbackCache();

    protected abstract void sendToChannel(ByteBuffer buf);
}
