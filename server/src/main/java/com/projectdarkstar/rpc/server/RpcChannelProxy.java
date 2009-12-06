package com.projectdarkstar.rpc.server;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
import com.projectdarkstar.rpc.common.RemoteCall;
import com.sun.sgs.app.ManagedReference;

import java.io.Serializable;

public class RpcChannelProxy implements RpcChannel, Serializable {

    private final ManagedReference<ServerChannelRpcListener> listener;

    RpcChannelProxy(ManagedReference<ServerChannelRpcListener> listener) {
        this.listener = listener;
    }

    public RemoteCall newRpcController() {
        return listener.get().newRpcController();
    }

    public void callMethod(MethodDescriptor method,
                           RpcController controller,
                           Message request,
                           Message responsePrototype,
                           RpcCallback<Message> done) {

        listener.get().callMethod(method, controller, request, responsePrototype, done);
    }
}
