package com.projectdarkstar.rpc.common;

import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.Service;
import org.apache.commons.lang.Validate;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public final class LocalService implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int serviceId;
    private final Class<? extends Service> serviceClass;
    private final Class<?> serviceInterfaceClass;
    private final Serializable rawService;

    public LocalService(int serviceId, Class<? extends Service> serviceClass, Class<?> serviceInterfaceClass, Serializable rawService) {
        Validate.isTrue(serviceClass.equals(serviceInterfaceClass.getEnclosingClass()));
        Validate.isTrue(serviceInterfaceClass.isInstance(rawService));

        this.serviceId = serviceId;
        this.serviceClass = serviceClass;
        this.serviceInterfaceClass = serviceInterfaceClass;
        this.rawService = rawService;
    }

    public int getServiceId() {
        return serviceId;
    }

    public Class<? extends Service> getServiceClass() {
        return serviceClass;
    }

    public Class<?> getServiceInterfaceClass() {
        return serviceInterfaceClass;
    }

    public Serializable getRawService() {
        return rawService;
    }

    public Service getService() {
        return getService(serviceClass, serviceInterfaceClass, rawService);
    }

    public ServiceDescriptor getServiceDescriptor() {
        return getServiceDescriptor(serviceClass);
    }

    public static Service getService(final Class<? extends Service> serviceClass,
                                     final Class<?> serviceInterfaceClass,
                                     final Serializable rawService) {
        try {

            final Method method = serviceClass.getMethod("newReflectiveService", serviceInterfaceClass);
            return (Service) method.invoke(null, rawService);

        } catch (NoSuchMethodException e) {
            throw new RuntimeException("No newReflectiveService() method on class: " + serviceClass.getName(), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public static ServiceDescriptor getServiceDescriptor(Class<? extends Service> serviceClass) {
        try {

            final Method method = serviceClass.getMethod("getDescriptor");
            return (ServiceDescriptor) method.invoke(null);

        } catch (NoSuchMethodException e) {
            throw new RuntimeException("No getDescriptor() method on class: " + serviceClass.getName(), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
