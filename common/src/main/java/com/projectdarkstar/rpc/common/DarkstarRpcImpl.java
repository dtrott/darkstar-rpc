package com.projectdarkstar.rpc.common;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
import com.projectdarkstar.rpc.CoreRpc.*;
import com.projectdarkstar.rpc.CoreRpc;
import org.apache.commons.lang.Validate;

import java.io.Serializable;

public abstract class DarkstarRpcImpl implements DarkstarRpc, RpcChannel, Serializable {

    protected final NamingService namingService;

    public RpcChannel getRpcChannel() {
        return this;
    }

    protected DarkstarRpcImpl(NamingService namingService) {
        this.namingService = namingService;
    }

    public void callMethod(Descriptors.MethodDescriptor method, RpcController rpcController,
                           Message request, Message responsePrototype, RpcCallback<Message> messageRpcCallback) {

        Validate.notNull(method, "callMethod(): method is null");
        Validate.notNull(rpcController, "callMethod(): rpcController is null");
        Validate.notNull(request, "callMethod(): request is null");
        Validate.notNull(responsePrototype, "callMethod(): responsePrototype is null");

        if (!(rpcController instanceof RemoteCall)) {
            throw new RuntimeException("Invalid RpcController");
        }

        RemoteCall remoteCall = (RemoteCall) rpcController;

        final int serviceId = namingService.getServiceId(method.getService());
        final int methodId = namingService.getMethodId(serviceId, method.getName());

        remoteCall.setCallback(responsePrototype, messageRpcCallback);
        final int requestId = remoteCall.getRequestId();

        final Header.Builder headerBuilder = Header.newBuilder();
        headerBuilder.setMessageType(Header.MessageType.SYNC_REQUEST);
        headerBuilder.setRequestId(requestId);
        headerBuilder.setServiceId(serviceId);
        headerBuilder.setMethodId(methodId);

        sendMessage(headerBuilder.build(), request);
    }

    protected abstract void sendMessage(Header header, Message request);
}
