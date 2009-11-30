package com.projectdarkstar.rpc.common;

import com.google.protobuf.Descriptors.ServiceDescriptor;

import java.io.Serializable;

public interface NamingService extends Serializable {
    int getServiceId(ServiceDescriptor service);

    int getMethodId(int serviceId, String methodName);

    String getMethodName(int serviceId, int methodId);
}
