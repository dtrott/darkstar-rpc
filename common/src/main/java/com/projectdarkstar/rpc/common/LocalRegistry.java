package com.projectdarkstar.rpc.common;

import com.projectdarkstar.rpc.CoreRpc.Header;

import java.io.Serializable;
import java.nio.ByteBuffer;

public interface LocalRegistry extends Serializable {
    <T> void registerService(Class<T> serviceInterfaceClass, T service);

    void receivedRequest(Header header, ByteBuffer message);
}
