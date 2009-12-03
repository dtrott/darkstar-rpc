package com.projectdarkstar.rpc.client;

import com.projectdarkstar.rpc.common.CallbackCache;
import com.projectdarkstar.rpc.common.RemoteCall;
import com.projectdarkstar.rpc.common.RemoteCallImpl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ClientCallbackCache implements CallbackCache, Serializable {
    private final Map<Integer, RemoteCall> callbacks;
    protected int nextRequestId;

    public ClientCallbackCache() {
        this.callbacks = new HashMap<Integer, RemoteCall>();
        this.nextRequestId = 1;
    }

    public synchronized int getNextRequestId() {
        return this.nextRequestId++;
    }

    @Override
    public RemoteCall newRpcController() {
        final int requestId = getNextRequestId();
        RemoteCallImpl remoteRpcCall = new RemoteCallImpl(requestId);

        synchronized (callbacks) {
            callbacks.put(requestId, remoteRpcCall);
        }

        return remoteRpcCall;
    }

    @Override
    public RemoteCall removeCallback(int requestId) {
        synchronized (callbacks) {
            return callbacks.remove(requestId);
        }
    }
}
