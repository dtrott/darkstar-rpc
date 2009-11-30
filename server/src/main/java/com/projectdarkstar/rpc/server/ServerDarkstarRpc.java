package com.projectdarkstar.rpc.server;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
import com.google.protobuf.Message;
import com.projectdarkstar.rpc.common.CallbackCache;
import com.projectdarkstar.rpc.common.DarkstarRpc;
import com.projectdarkstar.rpc.common.DarkstarRpcImpl;
import com.projectdarkstar.rpc.common.NamingService;
import com.sun.sgs.app.AppContext;
import com.sun.sgs.app.ManagedReference;
import org.apache.commons.lang.NotImplementedException;

import java.io.Serializable;

class ServerDarkstarRpc extends DarkstarRpcImpl implements DarkstarRpc, RpcChannel, Serializable {

    private final ManagedReference<ServerChannelRpcListener> listener;
    private final CallbackCache serverCallbackCache;

    ServerDarkstarRpc(final ServerChannelRpcListener listener,
                      final ServerCallbackCache serverCallbackCache,
                      final NamingService namingService) {
        super(namingService);

        this.listener = AppContext.getDataManager().createReference(listener);
        this.serverCallbackCache = serverCallbackCache;
    }

    public <T> void registerService(Class<T> serviceInterfaceClass, T service) {
        listener.getForUpdate().getLocal().registerService(serviceInterfaceClass, service);
    }

    @Override
    protected void sendRequest(int serviceId, int methodId, long requestId, Message request) {
        listener.get().sendRequest(serviceId, methodId,requestId, request);
    }

    @Override
    public BlockingRpcChannel getBlockingRpcChannel() {
        throw new NotImplementedException("Not Implemented: getBlockingRpcChannel()" +
            " Server does not (Currently) support blocking channels.");
    }

    public RpcController newRpcController() {
        return serverCallbackCache.newRpcController();
    }

}
