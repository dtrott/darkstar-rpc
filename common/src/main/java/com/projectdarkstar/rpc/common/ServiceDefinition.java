package com.projectdarkstar.rpc.common;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.projectdarkstar.rpc.CoreRpc.MethodDetails;
import com.projectdarkstar.rpc.CoreRpc.ServiceDetails;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ServiceDefinition implements Serializable {

    private final int serviceId;
    private final String serviceFullName;
    private final Map<String, Integer> methodsByName = new HashMap<String, Integer>();
    private final Map<Integer, String> methodsById = new HashMap<Integer, String>();

    public ServiceDefinition(int serviceId, ServiceDescriptor descriptor) {
        this.serviceId = serviceId;
        this.serviceFullName = descriptor.getFullName();

        for (final MethodDescriptor method : descriptor.getMethods()) {
            this.methodsByName.put(method.getName(), method.getIndex());
            this.methodsById.put(method.getIndex(), method.getName());
        }
    }

    public ServiceDefinition(ServiceDetails serviceDetails) {
        this.serviceId = serviceDetails.getId();
        this.serviceFullName = serviceDetails.getFullName();

        for (final MethodDetails details : serviceDetails.getMethodList()) {
            methodsByName.put(details.getName(), details.getId());
            methodsById.put(details.getId(), details.getName());
        }
    }

    public int getServiceId() {
        return serviceId;
    }

    public String getServiceFullName() {
        return serviceFullName;
    }

    public int getMethodId(String name) {
        return methodsByName.get(name);
    }

    public String getMethodName(int id) {
        return methodsById.get(id);
    }

    public ServiceDetails buildServiceDetails() {

        final ServiceDetails.Builder serviceDetails = ServiceDetails.newBuilder();

        serviceDetails.setId(this.serviceId);
        serviceDetails.setFullName(this.serviceFullName);

        for (Map.Entry<Integer, String> entry : methodsById.entrySet()) {
            final MethodDetails.Builder methodDetails = MethodDetails.newBuilder();
            methodDetails.setId(entry.getKey());
            methodDetails.setName(entry.getValue());
            serviceDetails.addMethod(methodDetails);
        }

        return serviceDetails.build();
    }
}
