package com.projectdarkstar.rpc.server;

import com.projectdarkstar.rpc.common.CallbackCache;
import com.projectdarkstar.rpc.common.RemoteCall;
import com.projectdarkstar.rpc.common.RemoteCallImpl;
import com.sun.sgs.app.AppContext;
import com.sun.sgs.app.DataManager;
import com.sun.sgs.app.ManagedReference;
import com.sun.sgs.app.util.ManagedSerializable;
import com.sun.sgs.app.util.ScalableHashMap;

import java.io.Serializable;

public class ServerCallbackCache implements CallbackCache, Serializable {
    private static final long serialVersionUID = 1L;

    private static class RemoteCallMap extends ScalableHashMap<Integer, ManagedSerializable<RemoteCall>> {
        private static final long serialVersionUID = 1L;
    }

    private final ManagedReference<ManagedSerializable<Integer>> nextRequestId;
    private final ManagedReference<RemoteCallMap> callbacks;

    ServerCallbackCache() {
        final DataManager dataManager = AppContext.getDataManager();
        this.nextRequestId = dataManager.createReference(new ManagedSerializable<Integer>(1));
        this.callbacks = dataManager.createReference(new RemoteCallMap());
    }

    private int getNextRequestId() {
        final ManagedSerializable<Integer> nextValue = nextRequestId.getForUpdate();
        final int value = nextValue.get();
        nextValue.set(value + 1);
        return value;
    }

    @Override
    public RemoteCall newRpcController() {
        final int requestId = getNextRequestId();

        final ManagedSerializable<RemoteCall> managedRemoteCall =
            new ManagedSerializable<RemoteCall>(new RemoteCallImpl(requestId));

        callbacks.getForUpdate().put(requestId, managedRemoteCall);

        return new RemoteCallWrapper(managedRemoteCall);
    }

    @Override
    public RemoteCall removeCallback(int requestId) {
        final ManagedSerializable<RemoteCall> managedCall = callbacks.getForUpdate().remove(requestId);
        AppContext.getDataManager().removeObject(managedCall);
        return managedCall.get();
    }
}
