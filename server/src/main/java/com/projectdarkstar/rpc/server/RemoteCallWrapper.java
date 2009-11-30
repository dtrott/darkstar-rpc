package com.projectdarkstar.rpc.server;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.projectdarkstar.rpc.common.RemoteCall;
import com.sun.sgs.app.AppContext;
import com.sun.sgs.app.ManagedReference;
import com.sun.sgs.app.util.ManagedSerializable;

public class RemoteCallWrapper implements RemoteCall {
    private final ManagedReference<ManagedSerializable<RemoteCall>> delegate;

    public RemoteCallWrapper(ManagedSerializable<RemoteCall> remoteRpcCall) {
        this.delegate = AppContext.getDataManager().createReference(remoteRpcCall);
    }

    public long getRequestId() {
        return get().getRequestId();
    }

    public Message getResponsePrototype() {
        return get().getResponsePrototype();
    }

    public RpcCallback<Message> getCallback() {
        return get().getCallback();
    }

    public void setCallback(Message responsePrototype, RpcCallback<Message> callback) {
        getForUpdate().setCallback(responsePrototype, callback);
    }

    public void reset() {
        getForUpdate().reset();
    }

    public boolean failed() {
        return getForUpdate().failed();
    }

    public String errorText() {
        return get().errorText();
    }

    public void startCancel() {
        getForUpdate().startCancel();
    }

    public void setFailed(String reason) {
        getForUpdate().setFailed(reason);
    }

    public boolean isCanceled() {
        return get().isCanceled();
    }

    public void notifyOnCancel(RpcCallback<Object> callback) {
        getForUpdate().notifyOnCancel(callback);
    }

    private RemoteCall get() {
        return delegate.get().get();
    }

    private RemoteCall getForUpdate() {
        return delegate.getForUpdate().get();
    }
}
