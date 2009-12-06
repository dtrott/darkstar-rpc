package com.projectdarkstar.rpc.client;

import com.google.protobuf.Descriptors;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.projectdarkstar.rpc.CoreRpc.MetaService;
import com.projectdarkstar.rpc.CoreRpc.ServiceName;
import com.projectdarkstar.rpc.common.NamingService;
import com.projectdarkstar.rpc.util.ServiceDefinition;

import java.util.HashMap;
import java.util.Map;

public class ClientNamingService implements NamingService {

    private final Map<Integer, ServiceDefinition> servicesById;
    private final Map<String, ServiceDefinition> servicesByFullName;
    private ClientChannelRpcListener clientListener;
    private MetaService.BlockingInterface metaService;

    public ClientNamingService() {
        this.servicesById = new HashMap<Integer, ServiceDefinition>();
        this.servicesByFullName = new HashMap<String, ServiceDefinition>();

        // Force the local definition of the meta service.
        addServiceDefinition(new ServiceDefinition(0, MetaService.getDescriptor()));
    }

    public void addServiceDefinition(ServiceDefinition serviceDefinition) {
        this.servicesById.put(serviceDefinition.getServiceId(), serviceDefinition);
        this.servicesByFullName.put(serviceDefinition.getServiceFullName(), serviceDefinition);
    }

    public void setClientListener(ClientChannelRpcListener clientListener) {
        this.clientListener = clientListener;
        this.metaService = MetaService.newBlockingStub(clientListener);
    }

    @Override
    public int getServiceId(Descriptors.ServiceDescriptor service) {
        final String fullName = service.getFullName();

        ServiceDefinition serviceDefinition = servicesByFullName.get(fullName);

        if (serviceDefinition == null) {
            serviceDefinition = getServiceDetails(fullName);
            servicesById.put(serviceDefinition.getServiceId(), serviceDefinition);
            servicesByFullName.put(fullName, serviceDefinition);
        }

        return serviceDefinition.getServiceId();
    }

    @Override
    public int getMethodId(int serviceId, String methodName) {
        return getServiceInfoById(serviceId).getMethodId(methodName);
    }

    @Override
    public String getMethodName(int serviceId, int methodId) {
        return getServiceInfoById(serviceId).getMethodName(methodId);
    }

    private ServiceDefinition getServiceInfoById(int serviceId) {
        final ServiceDefinition serviceDefinition = servicesById.get(serviceId);

        if (serviceDefinition == null) {
            throw new RuntimeException("Unknown ServiceId: " + serviceId);
        }

        return serviceDefinition;
    }

    ServiceDefinition getServiceDetails(final String name) {
        try {
            final RpcController rpcController = clientListener.newRpcController();
            final ServiceName request = ServiceName.newBuilder().setFullName(name).build();
            return new ServiceDefinition(metaService.lookupService(rpcController, request));
        } catch (ServiceException e) {
            throw new RuntimeException("Unabled to lookup service definition: " + name, e);
        }
    }
}
