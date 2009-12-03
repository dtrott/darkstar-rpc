package com.projectdarkstar.rpc.common;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import java.io.Serializable;

public interface RemoteCall extends RpcController, Serializable {
    int getRequestId();

    Message getResponsePrototype();

    RpcCallback<Message> getCallback();

    void setCallback(Message responsePrototype, RpcCallback<Message> callback);
}
