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

    private static class RemoteCallMap extends ScalableHashMap<Long, ManagedSerializable<RemoteCall>> {
        private static final long serialVersionUID = 1L;
    }

    private final ManagedReference<ManagedSerializable<Long>> nextRequestId;
    private final ManagedReference<RemoteCallMap> callbacks;

    ServerCallbackCache() {
        final DataManager dataManager = AppContext.getDataManager();
        this.nextRequestId = dataManager.createReference(new ManagedSerializable<Long>(1L));
        this.callbacks = dataManager.createReference(new RemoteCallMap());
    }

    private long getNextRequestId() {
        final ManagedSerializable<Long> nextValue = nextRequestId.getForUpdate();
        final long value = nextValue.get();
        nextValue.set(value + 1L);
        return value;
    }

    @Override
    public RemoteCall newRpcController() {
        final long requestId = getNextRequestId();

        final ManagedSerializable<RemoteCall> managedRemoteCall =
            new ManagedSerializable<RemoteCall>(new RemoteCallImpl(requestId));

        callbacks.getForUpdate().put(requestId, managedRemoteCall);

        return new RemoteCallWrapper(managedRemoteCall);
    }

    @Override
    public RemoteCall removeCallback(long requestId) {
        final ManagedSerializable<RemoteCall> managedCall = callbacks.getForUpdate().remove(requestId);
        AppContext.getDataManager().removeObject(managedCall);
        return managedCall.get();
    }
}
