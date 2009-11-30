package com.projectdarkstar.rpc.client;

import com.projectdarkstar.rpc.common.CallbackCache;
import com.projectdarkstar.rpc.common.RemoteCall;
import com.projectdarkstar.rpc.common.RemoteCallImpl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ClientCallbackCache implements CallbackCache, Serializable {
    private final Map<Long, RemoteCall> callbacks;
    protected long nextRequestId;

    public ClientCallbackCache() {
        this.callbacks = new HashMap<Long, RemoteCall>();
        this.nextRequestId = 1;
    }

    public synchronized long getNextRequestId() {
        return this.nextRequestId++;
    }

    @Override
    public RemoteCall newRpcController() {
        final long requestId = getNextRequestId();
        RemoteCallImpl remoteRpcCall = new RemoteCallImpl(requestId);

        synchronized (callbacks) {
            callbacks.put(requestId, remoteRpcCall);
        }

        return remoteRpcCall;
    }

    @Override
    public RemoteCall removeCallback(long requestId) {
        synchronized (callbacks) {
            return callbacks.remove(requestId);
        }
    }
}
