package com.projectdarkstar.rpc.server;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcUtil;
import com.google.protobuf.SerializableRpcCallbackProvider;
import com.projectdarkstar.rpc.CoreRpc.MetaService;
import com.projectdarkstar.rpc.common.AbstractChannelListener;
import com.projectdarkstar.rpc.common.RemoteCall;
import com.projectdarkstar.rpc.util.RemoteCallImpl;
import com.sun.sgs.app.AppContext;
import com.sun.sgs.app.Channel;
import com.sun.sgs.app.ChannelListener;
import com.sun.sgs.app.ClientSession;
import com.sun.sgs.app.DataManager;
import com.sun.sgs.app.ManagedObject;
import com.sun.sgs.app.ManagedReference;
import com.sun.sgs.app.util.ManagedSerializable;
import com.sun.sgs.app.util.ScalableHashMap;

import java.nio.ByteBuffer;

public class ServerChannelRpcListener extends AbstractChannelListener implements ChannelListener, ManagedObject {
    static {
        RpcUtil.setRpcCallbackProvider(new SerializableRpcCallbackProvider());
    }

    private static class RemoteCallMap extends ScalableHashMap<Integer, ManagedSerializable<RemoteCall>> {
        private static final long serialVersionUID = 1L;
    }

    private static final long serialVersionUID = 1L;

    private final ManagedReference<RemoteCallMap> callbacks;
    private final ManagedReference<ManagedSerializable<Integer>> nextRequestId;
    private volatile ManagedReference<Channel> channel;

    public ServerChannelRpcListener(final ServerNamingService namingService) {
        super(namingService);

        final DataManager dataManager = AppContext.getDataManager();

        this.callbacks = dataManager.createReference(new RemoteCallMap());
        this.nextRequestId = dataManager.createReference(new ManagedSerializable<Integer>(1));

        registerService(MetaService.Interface.class, namingService);
    }

    public void setChannel(Channel channel) {
        this.channel = AppContext.getDataManager().createReference(channel);
    }

    // DarkstarRpc Implementation

    public RemoteCall newRpcController() {
        final int requestId = getNextRequestId();

        final ManagedSerializable<RemoteCall> managedRemoteCall =
            new ManagedSerializable<RemoteCall>(new RemoteCallImpl(requestId));

        callbacks.getForUpdate().put(requestId, managedRemoteCall);

        return new RemoteCallWrapper(managedRemoteCall);
    }

    private int getNextRequestId() {
        final ManagedSerializable<Integer> nextValue = nextRequestId.getForUpdate();
        final int value = nextValue.get();
        nextValue.set(value + 1);
        return value;
    }

    // ChannelListener

    @Override
    public void receivedMessage(Channel channel, ClientSession sender, ByteBuffer message) {
        receivedMessage(message);
    }

    // Protected
    @Override
    protected RpcCallback<Message> newResponseCallback(int requestId) {
        return new ServerRpcCallback(this, requestId);
    }

    @Override
    protected RemoteCall removeCallback(int requestId) {
        final ManagedSerializable<RemoteCall> managedCall = callbacks.getForUpdate().remove(requestId);
        AppContext.getDataManager().removeObject(managedCall);
        return managedCall.get();
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
}
