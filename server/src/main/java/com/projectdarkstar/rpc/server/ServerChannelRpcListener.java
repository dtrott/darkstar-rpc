package com.projectdarkstar.rpc.server;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.RpcUtil;
import com.google.protobuf.SerializableRpcCallbackProvider;
import com.projectdarkstar.rpc.CoreRpc.MetaService;
import com.projectdarkstar.rpc.common.AbstractChannelController;
import com.projectdarkstar.rpc.common.ChannelController;
import com.projectdarkstar.rpc.common.DarkstarRpc;
import com.projectdarkstar.rpc.common.LocalRegistry;
import com.projectdarkstar.rpc.common.LocalRegistryImpl;
import com.projectdarkstar.rpc.common.NamingService;
import com.projectdarkstar.rpc.common.CallbackCache;
import com.sun.sgs.app.AppContext;
import com.sun.sgs.app.Channel;
import com.sun.sgs.app.ChannelListener;
import com.sun.sgs.app.ClientSession;
import com.sun.sgs.app.ManagedObject;
import com.sun.sgs.app.ManagedReference;
import org.apache.commons.lang.Validate;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class ServerChannelRpcListener implements ChannelListener, ManagedObject, Serializable {
    private static final long serialVersionUID = 1L;

    private final ServerCallbackCache callbackCache;
    private final ChannelController controller;
    private final LocalRegistry local;
    private final NamingService namingService;

    private volatile ManagedReference<Channel> channel;

    static {
        RpcUtil.setRpcCallbackProvider(new SerializableRpcCallbackProvider());
    }

    private class ServerChannelController extends AbstractChannelController implements Serializable {
        @Override
        protected CallbackCache getCallbackCache() {
            return callbackCache;
        }

        protected void sendToChannel(ByteBuffer buf) {
            try {
                if (channel == null) {
                    throw new IllegalStateException("No connection");
                }
                channel.get().send(null, buf);
            } catch (IllegalStateException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public RpcCallback<Message> newResponseCallback(final int serviceId, final long requestId) {
            return new RpcCallbackWrapper(ServerChannelRpcListener.this, serviceId, requestId);
        }
    }

    public ServerChannelRpcListener(final ServerNamingService namingService) {
        Validate.notNull(namingService, "namingService is null");

        this.callbackCache = new ServerCallbackCache();
        this.controller = new ServerChannelController();
        this.local = new LocalRegistryImpl(namingService, controller);
        this.namingService = namingService;
        local.registerService(MetaService.Interface.class, namingService);
    }

    public DarkstarRpc getDarkstarRpc() {
        return new ServerDarkstarRpc(this, callbackCache, namingService);
    }

    ChannelController getController() {
        return controller;
    }

    public RpcController newRpcController() {
        return callbackCache.newRpcController();
    }

    public void setChannel(Channel channel) {
        this.channel = AppContext.getDataManager().createReference(channel);
    }

    public void receivedMessage(Channel channel, ClientSession sender, ByteBuffer message) {
        controller.receivedMessage(message, local);
    }

    public void sendRequest(int serviceId, int methodId, long requestId, Message request) {
        controller.sendRequest(serviceId, methodId, requestId, request);
    }

    LocalRegistry getLocal() {
        return local;
    }
}
