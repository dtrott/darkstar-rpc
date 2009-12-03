package com.projectdarkstar.rpc.common;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.projectdarkstar.rpc.CoreRpc.Header;

import java.nio.ByteBuffer;

public interface ChannelController {
    void receivedMessage(ByteBuffer message, LocalRegistry local);

    void sendResponse(int requestId, Message message);

    /**
     * Sends a message to the remote system.
     * @param header the message header.
     * @param message the message itself.
     */
    void sendMessage(Header header, Message message);

    RpcCallback<Message> newResponseCallback(final int requestId);
}
