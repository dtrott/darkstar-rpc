package com.projectdarkstar.rpc.server;

import com.google.protobuf.Descriptors;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.projectdarkstar.rpc.CoreRpc.MetaService;
import com.projectdarkstar.rpc.CoreRpc.ServiceDetails;
import com.projectdarkstar.rpc.CoreRpc.ServiceName;
import com.sun.sgs.app.ManagedReference;

class ServerNamingServiceWrapper implements ServerNamingService, MetaService.Interface {

    private final ManagedReference<ServerNamingService> delegate;

    ServerNamingServiceWrapper(final ManagedReference<ServerNamingService> delegate) {
        this.delegate = delegate;
    }

    @Override
    public int getServiceId(Descriptors.ServiceDescriptor service) {
        return get().getServiceId(service);
    }

    @Override
    public String getMethodName(int serviceId, int methodId) {
        return get().getMethodName(serviceId, methodId);
    }

    @Override
    public int getMethodId(int serviceId, String methodName) {
        return get().getMethodId(serviceId, methodName);
    }

    @Override
    public ServiceDetails getServiceDetails(String fullName) {
        return get().getServiceDetails(fullName);
    }

    @Override
    public void lookupService(RpcController controller, ServiceName request, RpcCallback<ServiceDetails> done) {
        get().lookupService(controller, request, done);
    }

    private ServerNamingService get() {
        return delegate.get();
    }

}
