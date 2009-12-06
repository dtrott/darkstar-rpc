package com.projectdarkstar.rpc.server;

import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.projectdarkstar.rpc.CoreRpc;
import com.projectdarkstar.rpc.util.ServiceDefinition;
import com.projectdarkstar.rpc.CoreRpc.MetaService;
import com.projectdarkstar.rpc.CoreRpc.ServiceDetails;
import com.projectdarkstar.rpc.common.NamingService;
import com.sun.sgs.app.AppContext;
import com.sun.sgs.app.ManagedObject;

import java.util.HashMap;
import java.util.Map;

public class ServerNamingServiceImpl implements ServerNamingService, NamingService, ManagedObject {
    private static final int META_SERVICE_ID = 0;
    private static final String META_SERVICE_NAME = MetaService.getDescriptor().getFullName();

    private final Map<Integer, ServiceDefinition> servicesById;
    private final Map<String, ServiceDefinition> servicesByFullName;

    ServerNamingServiceImpl() {
        servicesById = new HashMap<Integer, ServiceDefinition>();
        servicesByFullName = new HashMap<String, ServiceDefinition>();

        // Force the system to load the metadata first.
        getServiceId(MetaService.getDescriptor());
    }

    @Override
    public int getServiceId(ServiceDescriptor serviceDescriptor) {
        final String fullName = serviceDescriptor.getFullName();

        ServiceDefinition serviceDefinition = servicesByFullName.get(fullName);

        if (serviceDefinition == null) {
            AppContext.getDataManager().markForUpdate(this);

            final int serviceId;
            if (fullName.equals(META_SERVICE_NAME)) {
                serviceId = META_SERVICE_ID;
            } else {
                serviceId = servicesByFullName.size();
            }
            serviceDefinition = new ServiceDefinition(serviceId, serviceDescriptor);
            servicesById.put(serviceId, serviceDefinition);
            servicesByFullName.put(fullName, serviceDefinition);
        }

        return serviceDefinition.getServiceId();
    }

    @Override
    public String getMethodName(int serviceId, int methodId) {
        return getService(serviceId).getMethodName(methodId);
    }

    @Override
    public int getMethodId(int serviceId, String methodName) {
        return getService(serviceId).getMethodId(methodName);
    }

    @Override
    public void lookupService(RpcController controller, CoreRpc.ServiceName request, RpcCallback<ServiceDetails> done) {
        done.run(getServiceDetails(request.getFullName()));
    }

    public ServiceDetails getServiceDetails(String fullName) {
        return servicesByFullName.get(fullName).buildServiceDetails();
    }

    private ServiceDefinition getService(int serviceId) {
        return servicesById.get(serviceId);
    }
}
