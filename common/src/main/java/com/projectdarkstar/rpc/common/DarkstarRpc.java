package com.projectdarkstar.rpc.common;

import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
import com.google.protobuf.BlockingRpcChannel;

public interface DarkstarRpc {
    <T> void registerService(Class<T> serviceInterfaceClass, T service);

    RpcChannel getRpcChannel();

    BlockingRpcChannel getBlockingRpcChannel();

    RpcController newRpcController();
}
