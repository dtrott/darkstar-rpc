package com.projectdarkstar.rpc.common;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;

import java.nio.ByteBuffer;

public interface ChannelController {
    /**
     * Sends an RPC Request.
     *
     * @param serviceId        the service to send the message to.
     * @param methodId         the method to call.
     * @param requestId        a unique id for the request.
     * @param requestParameter the request itself.
     */
    void sendRequest(int serviceId, int methodId, long requestId, Message requestParameter);

    /**
     * Sends an RPC Response.
     *
     * @param serviceId the service to send the message to.
     * @param requestId a unique id for the request.
     * @param response  the response from the call.
     */
    void sendResponse(int serviceId, long requestId, Message response);

    RpcCallback<Message> newResponseCallback(final int serviceId, final long requestId);

    void receivedMessage(ByteBuffer message, LocalRegistry local);
}
