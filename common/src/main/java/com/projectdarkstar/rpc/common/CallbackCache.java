package com.projectdarkstar.rpc.common;

import java.io.Serializable;

public interface CallbackCache extends Serializable {

    RemoteCall newRpcController();

    RemoteCall removeCallback(long requestId);
}
