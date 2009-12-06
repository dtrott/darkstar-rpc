package com.projectdarkstar.rpc.client;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.RpcUtil;
import com.google.protobuf.ServiceException;
import com.projectdarkstar.rpc.common.AbstractChannelListener;
import com.projectdarkstar.rpc.common.RemoteCall;
import com.projectdarkstar.rpc.util.RemoteCallImpl;
import com.sun.sgs.client.ClientChannel;
import com.sun.sgs.client.ClientChannelListener;
import org.apache.commons.lang.Validate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Client side channel listener.
 * <p/>
 * The client side is much simpler than the server as it doesn't have to worry about ManagedObject's and Serialization.
 */
public class ClientChannelRpcListener extends AbstractChannelListener implements ClientChannelListener, BlockingRpcChannel {

    /**
     * Cache of outgoing calls, which is used to route responses back to the correct callback.
     */
    private final Map<Integer, RemoteCall> callbacks;

    /**
     * id of the next outgoing RPC request.
     */
    protected int nextRequestId;

    /**
     * The underlying channel used to send messages.
     */
    private ClientChannel channel;

    /**
     * @param namingService the naming service used to map service names to id's.
     */
    public ClientChannelRpcListener(final ClientNamingService namingService) {
        super(namingService);
        Validate.notNull(namingService, "namingService is null");

        this.callbacks = new HashMap<Integer, RemoteCall>();
        this.nextRequestId = 1;

        namingService.setClientListener(this);
    }

    public void setChannel(ClientChannel channel) {
        if (this.channel != null) {
            throw new IllegalStateException("setChannel() this.channel should be null");
        }
        this.channel = channel;
    }

    // ClientChannelListener

    @Override
    public void receivedMessage(ClientChannel channel, ByteBuffer message) {
        receivedMessage(message);
    }

    @Override
    public void leftChannel(ClientChannel channel) {
        if (this.channel != channel) {
            throw new IllegalStateException("leftChannel() called with incorrect channel");
        }
        this.channel = null;
    }

    // DarkstarRpc

    public RemoteCall newRpcController() {
        final int requestId = getNextRequestId();
        RemoteCallImpl remoteRpcCall = new RemoteCallImpl(requestId);

        synchronized (callbacks) {
            callbacks.put(requestId, remoteRpcCall);
        }

        return remoteRpcCall;
    }

    private synchronized int getNextRequestId() {
        return this.nextRequestId++;
    }

    // RPC Blocking Channel

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

        private Message getMessage(RpcController controller) throws ServiceException {
            try {
                latch.await();
                return message;
            } catch (InterruptedException e) {
                controller.setFailed(e.getMessage());
                throw new ServiceException(e.getMessage());
            }
        }
    }

    // Protected Methods

    /**
     * Generates a proxy to route the response of a local call back to the remote system.
     * <p/>
     * The object returned is a flyweight that sits over the channel.
     *
     * @param requestId the id of the original request that this is a response to.
     * @return the new callback object.
     */
    protected RpcCallback<Message> newResponseCallback(final int requestId) {

        return new RpcCallback<Message>() {
            @Override
            public void run(Message message) {
                sendResponse(requestId, message);
            }
        };
    }

    @Override
    protected RemoteCall removeCallback(int requestId) {
        synchronized (callbacks) {
            return callbacks.remove(requestId);
        }
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
}
