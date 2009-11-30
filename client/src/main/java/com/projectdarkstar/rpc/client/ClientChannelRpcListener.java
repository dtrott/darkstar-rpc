package com.projectdarkstar.rpc.client;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.RpcUtil;
import com.google.protobuf.ServiceException;
import com.projectdarkstar.rpc.common.AbstractChannelController;
import com.projectdarkstar.rpc.common.CallbackCache;
import com.projectdarkstar.rpc.common.DarkstarRpc;
import com.projectdarkstar.rpc.common.DarkstarRpcImpl;
import com.projectdarkstar.rpc.common.LocalRegistry;
import com.projectdarkstar.rpc.common.LocalRegistryImpl;
import com.sun.sgs.client.ClientChannel;
import com.sun.sgs.client.ClientChannelListener;
import org.apache.commons.lang.Validate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

public class ClientChannelRpcListener extends DarkstarRpcImpl
    implements ClientChannelListener, BlockingRpcChannel, DarkstarRpc {

    private final ClientChannelController controller;
    private final CallbackCache callbackCache;
    private final LocalRegistryImpl local;

    private ClientChannel channel;

    public DarkstarRpc getDarkstarRpc() {
        return this;
    }

    private class ClientChannelController extends AbstractChannelController {
        @Override
        protected CallbackCache getCallbackCache() {
            return callbackCache;
        }

        protected void sendToChannel(ByteBuffer buf) {
            try {
                if (channel == null) {
                    throw new IllegalStateException("No connection");
                }
                channel.send(buf);
            } catch (IllegalStateException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public RpcCallback<Message> newResponseCallback(final int serviceId, final long requestId) {

            return new RpcCallback<Message>() {
                @Override
                public void run(Message message) {
                    controller.sendResponse(serviceId, requestId, message);
                }
            };
        }
    }

    public ClientChannelRpcListener(final ClientNamingService namingService) {
        super(namingService);
        Validate.notNull(namingService, "namingService is null");

        this.callbackCache = new ClientCallbackCache();
        this.controller = new ClientChannelController();
        this.local = new LocalRegistryImpl(namingService, controller);

        namingService.setDarkstarRpc(this);
    }

    @Override
    public <T> void registerService(Class<T> serviceInterfaceClass, T service) {
        local.registerService(serviceInterfaceClass, service);
    }

    public LocalRegistry getLocalRegistry() {
        return local;
    }

    public RpcController newRpcController() {
        return callbackCache.newRpcController();
    }

    public BlockingRpcChannel getBlockingRpcChannel() {
        return this;
    }

    public ClientChannelListener joinedChannel(ClientChannel channel) {
        assert this.channel == null;
        this.channel = channel;
        return this;
    }

    public void leftChannel(ClientChannel channel) {
        assert this.channel == channel;
        this.channel = null;
    }

    public void receivedMessage(ClientChannel channel, ByteBuffer message) {
        controller.receivedMessage(message, local);
    }

    public void sendRequest(int serviceId, int methodId, long requestId, Message request) {
        controller.sendRequest(serviceId, methodId, requestId, request);
    }

    @Override
    public Message callBlockingMethod(
        Descriptors.MethodDescriptor method, RpcController controller,
        Message request, Message responsePrototype) throws ServiceException {

        final RpcCallbackImpl callback = new RpcCallbackImpl();
        callMethod(method, controller, request, responsePrototype, RpcUtil.newOneTimeCallback(callback));
        return callback.getMessage(controller);
    }

    private static class RpcCallbackImpl implements RpcCallback<Message> {
        private final CountDownLatch latch;
        private volatile Message message;

        RpcCallbackImpl() {
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void run(Message message) {
            this.message = message;
            latch.countDown();
        }

        public Message getMessage(RpcController controller) throws ServiceException {
            try {
                latch.await();
                return message;
            } catch (InterruptedException e) {
                controller.setFailed(e.getMessage());
                throw new ServiceException(e.getMessage());
            }
        }
    }
}
