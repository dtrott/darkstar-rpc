package com.projectdarkstar.rpc.common;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.projectdarkstar.rpc.CoreRpc.Header;
import com.projectdarkstar.rpc.util.ByteBufferInputStream;
import com.projectdarkstar.rpc.util.LocalCallImpl;
import com.projectdarkstar.rpc.util.LocalService;
import org.apache.commons.lang.Validate;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Defines all channel listener functionality that can be implemented without knowledge of location (server or client).
 * <p/>
 * However some code needs to be implemented specifically on the server or the client.
 * As a result these are typically declared protected abstract so that the specific sub-class can implement
 * the functionality.
 */
public abstract class AbstractChannelListener implements Serializable, RpcChannel {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = Logger.getLogger(AbstractChannelListener.class);

    /**
     * Naming service used to map service and method names to numeric id's
     */
    private final NamingService namingService;

    /**
     * Defines the list of services that have been locally registered (can be called from remote side).
     */
    private final Map<Integer, LocalService> services;

    public AbstractChannelListener(NamingService namingService) {
        Validate.notNull(namingService, "namingService is null");
        this.namingService = namingService;
        this.services = new ConcurrentHashMap<Integer, LocalService>();
    }

    /**
     * Entry point for messages coming from the remote system.
     *
     * @param message the message to be processed.
     */
    protected void receivedMessage(ByteBuffer message) {
        Validate.notNull(message);
        final int headerLength = message.get();
        final byte[] header = new byte[headerLength];

        if (message.remaining() < headerLength) {
            logger.error("Header length mismatch, expected: " + headerLength + " actual: " + message.remaining());
            return;
        }

        // Load the header.
        message.get(header);

        final Header messageHeader;
        try {
            messageHeader = Header.newBuilder().mergeFrom(header).build();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Unable to parse message header", e);
        }

        switch (messageHeader.getMessageType()) {
            case ASYNC_REQUEST:
                break;
            case SYNC_REQUEST:
                receivedRequest(messageHeader, message);
                break;
            case RESPONSE:
                receivedResponse(messageHeader, message);
                break;
            default:
                logger.error("Unknown request received: " + messageHeader.getMessageType().toString());
                break;
        }
    }

    /**
     * Handler for request (requests to call a local service) messages.
     *
     * @param header     the message header (defines the method to call).
     * @param byteBuffer buffer containing the payload of he request.
     */
    private void receivedRequest(Header header, ByteBuffer byteBuffer) {
        RpcController rpcController = new LocalCallImpl();
        final int requestId = header.getRequestId();
        final int serviceId = header.getServiceId();
        final int methodId = header.getMethodId();

        final LocalService localService = services.get(serviceId);
        if (localService == null) {
            throw new AssertionError("Service Not Found serviceId: " + serviceId);
        }

        final Service service = localService.getService();
        final ServiceDescriptor serviceDescriptor = localService.getServiceDescriptor();
        final MethodDescriptor methodDescriptor = getMethodDescriptor(serviceDescriptor, serviceId, methodId);
        final Message requestPrototype = service.getRequestPrototype(methodDescriptor);

        final Message request = ByteBufferInputStream.buildMessage(requestPrototype, byteBuffer);

        localService.getService().callMethod(methodDescriptor, rpcController, request, newResponseCallback(requestId));
    }

    /**
     * Helper function to lookup a method descriptor using the naming service.
     *
     * @param serviceDescriptor the service containing the method.
     * @param serviceId         the id used as a key by the naming service
     * @param methodId          the method id for the message (again stored in the naming service).
     * @return the method descriptor for the method.
     */
    private MethodDescriptor getMethodDescriptor(ServiceDescriptor serviceDescriptor, int serviceId, int methodId) {
        return serviceDescriptor.findMethodByName(namingService.getMethodName(serviceId, methodId));
    }

    /**
     * Generates a proxy to route the response of a local call back to the remote system.
     * <p/>
     * The object returned is a flyweight that sits over the channel.
     *
     * @param requestId the id of the original request that this is a response to.
     * @return the new callback object.
     */
    protected abstract RpcCallback<Message> newResponseCallback(final int requestId);

    /**
     * Handles the response from an earlier remote call.
     *
     * @param header     the header containing the {@code requestId} of the original call.
     * @param byteBuffer the serialized for of the response object.
     */
    private void receivedResponse(Header header, ByteBuffer byteBuffer) {
        int requestId = header.getRequestId();
        final RemoteCall remoteCall = removeCallback(requestId);
        final RpcCallback<Message> callback = remoteCall.getCallback();
        final Message prototype = remoteCall.getResponsePrototype();

        callback.run(ByteBufferInputStream.buildMessage(prototype, byteBuffer));
    }

    /**
     * The RPC system has to track outgoing calls, this method removes the given tracked call.
     *
     * @param requestId the id of the callback to remove from the callback store.
     * @return the callback structure.
     */
    protected abstract RemoteCall removeCallback(int requestId);

    /**
     * Registers a service so that the remote side can call it.
     * <p/>
     * Services are looked up by type, hence there can only be one service registered for each type.
     *
     * @param serviceInterfaceClass the interface class generated by the protoc compiler.
     * @param service               your implementation of the service.
     * @param <T>                   the type of the interface.
     */
    public <T> void registerService(Class<T> serviceInterfaceClass, T service) {
        final Class<?> enclosingClass = serviceInterfaceClass.getEnclosingClass();

        Validate.isTrue(service instanceof Serializable, "Service implementation class is not serializable");
        Validate.isTrue(Service.class.isAssignableFrom(enclosingClass),
            "Invalid Service Interface: " + serviceInterfaceClass.getName());

        final Class<? extends Service> serviceClass = enclosingClass.asSubclass(Service.class);

        final ServiceDescriptor serviceDescriptor = LocalService.getServiceDescriptor(serviceClass);
        final int serviceId = namingService.getServiceId(serviceDescriptor);

        Validate.isTrue(!services.containsKey(serviceId));

        services.put(serviceId, new LocalService(
            serviceId, serviceClass, serviceInterfaceClass, (Serializable) service));
    }

    /**
     * Routes a method call to the channel to send it to the remote system.
     *
     * @param method             the method to call.
     * @param rpcController      the call structure (must implement {@link com.projectdarkstar.rpc.common.RemoteCall)}.
     * @param request            the request parameter for the function call.
     * @param responsePrototype  the prototype response.
     * @param messageRpcCallback the callback function to be called one the call is complete.
     */
    public void callMethod(MethodDescriptor method, RpcController rpcController,
                           Message request, Message responsePrototype, RpcCallback<Message> messageRpcCallback) {

        Validate.notNull(method, "callMethod(): method is null");
        Validate.notNull(rpcController, "callMethod(): rpcController is null");
        Validate.notNull(request, "callMethod(): request is null");
        Validate.notNull(responsePrototype, "callMethod(): responsePrototype is null");

        if (!(rpcController instanceof RemoteCall)) {
            throw new RuntimeException("Invalid RpcController");
        }

        RemoteCall remoteCall = (RemoteCall) rpcController;

        final int serviceId = namingService.getServiceId(method.getService());
        final int methodId = namingService.getMethodId(serviceId, method.getName());

        remoteCall.setCallback(responsePrototype, messageRpcCallback);
        final int requestId = remoteCall.getRequestId();

        final Header.Builder headerBuilder = Header.newBuilder();
        headerBuilder.setMessageType(Header.MessageType.SYNC_REQUEST);
        headerBuilder.setRequestId(requestId);
        headerBuilder.setServiceId(serviceId);
        headerBuilder.setMethodId(methodId);

        sendMessage(headerBuilder.build(), request);
    }

    /**
     * Wraps a response and sends it to the remote system.
     *
     * @param requestId the id of the original request.
     * @param message   the response to be sent.
     */
    public void sendResponse(final int requestId, final Message message) {
        final Header.Builder headerBuilder = Header.newBuilder();
        headerBuilder.setMessageType(Header.MessageType.RESPONSE);
        headerBuilder.setRequestId(requestId);
        sendMessage(headerBuilder.build(), message);
    }

    /**
     * Sends a message to the remote system.
     * <p/>
     * This could be a RPC call, a response to an earlier call or general housekeeping message.
     *
     * @param header  the message header.
     * @param message the message itself.
     */
    private void sendMessage(Header header, Message message) {
        final byte[] headerBytes = header.toByteArray();
        final byte[] messageBytes = message.toByteArray();

        ByteBuffer byteBuffer = ByteBuffer.allocate(1 + headerBytes.length + messageBytes.length);
        byteBuffer.put((byte) headerBytes.length);
        byteBuffer.put(headerBytes);
        byteBuffer.put(messageBytes);
        byteBuffer.flip();

        sendToChannel(byteBuffer);
    }

    /**
     * Low level connector method which puts the bytes on the wire.
     *
     * @param buffer the bytes to be sent.
     */
    protected abstract void sendToChannel(ByteBuffer buffer);
}
