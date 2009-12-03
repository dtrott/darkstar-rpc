package com.projectdarkstar.rpc.common;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.projectdarkstar.rpc.CoreRpc.Header;
import org.apache.commons.lang.Validate;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LocalRegistryImpl implements LocalRegistry, Serializable {
    private static final long serialVersionUID = 1L;

    private final NamingService namingService;
    private final ChannelController controller;
    private final Map<Integer, LocalService> services;

    public LocalRegistryImpl(NamingService namingService, ChannelController controller) {
        this.namingService = namingService;
        this.controller = controller;
        this.services = new ConcurrentHashMap<Integer, LocalService>();
    }

    @Override
    public <T> void registerService(Class<T> serviceInterfaceClass, T service) {
        final Class<?> enclosingClass = serviceInterfaceClass.getEnclosingClass();

        Validate.isTrue(service instanceof Serializable, "Service implementation class is not serializable");
        Validate.isTrue(Service.class.isAssignableFrom(enclosingClass),
            "Invalid Service Interface: " + serviceInterfaceClass.getName());

        final Class<? extends Service> serviceClass = enclosingClass.asSubclass(Service.class);

        final ServiceDescriptor serviceDescriptor = LocalService.getServiceDescriptor(serviceClass);
        final int serviceId = namingService.getServiceId(serviceDescriptor);

        Validate.isTrue(!services.containsKey(serviceId));

        services.put(serviceId, new LocalService(serviceId, serviceClass, serviceInterfaceClass, (Serializable) service));
    }

    @Override
    public void receivedRequest(Header header, ByteBuffer byteBuffer) {
        RpcController rpcController = newRpcController();
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

        localService.getService().callMethod(methodDescriptor, rpcController, request, controller.newResponseCallback(requestId));
    }

    private MethodDescriptor getMethodDescriptor(ServiceDescriptor serviceDescriptor, int serviceId, int methodId) {
        return serviceDescriptor.findMethodByName(namingService.getMethodName(serviceId, methodId));
    }

    private RpcController newRpcController() {
        return new LocalRpcController();
    }

    private static class LocalRpcController implements RpcController, Serializable {
        //TODO
        @Override
        public void reset() {
        }

        @Override
        public boolean failed() {
            return false;
        }

        @Override
        public String errorText() {
            return null;
        }

        @Override
        public void startCancel() {
        }

        @Override
        public void setFailed(String reason) {
        }

        @Override
        public boolean isCanceled() {
            return false;
        }

        @Override
        public void notifyOnCancel(RpcCallback<Object> callback) {
        }
    }
}
