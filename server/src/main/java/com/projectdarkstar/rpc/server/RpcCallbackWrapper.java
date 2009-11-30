package com.projectdarkstar.rpc.server;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.sun.sgs.app.AppContext;
import com.sun.sgs.app.ManagedReference;

import java.io.Serializable;

public class RpcCallbackWrapper implements RpcCallback<Message>, Serializable {
    private static final long serialVersionUID = 1L;

    private final ManagedReference<ServerChannelRpcListener> delegate;
    private final int serviceId;
    private final long requestId;

    public RpcCallbackWrapper(final ServerChannelRpcListener delegate, final int serviceId, final long requestId) {
        this.delegate = AppContext.getDataManager().createReference(delegate);
        this.serviceId = serviceId;
        this.requestId =requestId;
    }

    @Override
    public void run(Message message) {
        getForUpdate().getController().sendResponse(serviceId, requestId, message);
    }

    private ServerChannelRpcListener getForUpdate() {
        return delegate.getForUpdate();
    }

}
