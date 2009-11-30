package com.projectdarkstar.rpc.common;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
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
        final long requestId = remoteCall.getRequestId();

        sendRequest(serviceId, methodId, requestId, request);
    }

    protected abstract void sendRequest(int serviceId, int methodId, long requestId, Message request);
}
