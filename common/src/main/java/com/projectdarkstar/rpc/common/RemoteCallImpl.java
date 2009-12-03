package com.projectdarkstar.rpc.common;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class RemoteCallImpl implements RemoteCall {

    private final int requestId;
    private Class<? extends Message> responseClass;
    private RpcCallback<Message> callback;

    public RemoteCallImpl(int requestId) {
        this.requestId = requestId;
    }

    public int getRequestId() {
        return requestId;
    }

    public Message getResponsePrototype() {
        try {
            final Method method = responseClass.getMethod("getDefaultInstance");
            return (Message) method.invoke(null);

        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public RpcCallback<Message> getCallback() {
        return callback;
    }

    public void setCallback(Message responsePrototype, RpcCallback<Message> callback) {
        this.responseClass = responsePrototype.getClass();
        this.callback = callback;
    }

    // TODO

    @Override
    public void reset() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean failed() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String errorText() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void startCancel() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setFailed(String reason) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isCanceled() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void notifyOnCancel(RpcCallback<Object> callback) {
    }
}
