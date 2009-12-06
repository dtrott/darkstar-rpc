package com.projectdarkstar.rpc.server;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.sun.sgs.app.AppContext;
import com.sun.sgs.app.ManagedReference;

import java.io.Serializable;

/**
 * Callback on the server are more complex because the ServerRpcCallback is a Managed Object.
 *
 * As a result the wrapper needs to contain a managed reference and be Serializable itself.
 */
public class ServerRpcCallback implements RpcCallback<Message>, Serializable {
    private static final long serialVersionUID = 1L;

    private final ManagedReference<ServerChannelRpcListener> delegate;
    private final int requestId;

    public ServerRpcCallback(final ServerChannelRpcListener delegate, final int requestId) {
        this.delegate = AppContext.getDataManager().createReference(delegate);
        this.requestId = requestId;
    }

    @Override
    public void run(Message message) {
        get().sendResponse(requestId, message);
    }

    private ServerChannelRpcListener get() {
        return delegate.get();
    }
}
