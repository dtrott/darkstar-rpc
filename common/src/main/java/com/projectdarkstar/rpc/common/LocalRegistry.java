package com.projectdarkstar.rpc.common;

import com.google.protobuf.Service;

import java.io.Serializable;
import java.nio.ByteBuffer;

public interface LocalRegistry extends Serializable {
    <T> void registerService(Class<T> serviceInterfaceClass, T service);

    void receivedRequest(int serviceId, ByteBuffer message);
}
