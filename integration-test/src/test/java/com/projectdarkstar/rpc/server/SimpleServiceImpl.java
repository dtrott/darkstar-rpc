package com.projectdarkstar.rpc.server;

import com.example.Example.SimpleService;
import com.example.Example.StringPair;
import com.google.protobuf.RpcController;
import com.google.protobuf.RpcCallback;
import static com.projectdarkstar.rpc.server.TestUtils.buildPair;

import java.io.Serializable;

public class SimpleServiceImpl implements SimpleService.Interface, Serializable {
    @Override
    public void exchange(RpcController controller, StringPair request, RpcCallback<StringPair> done) {
        done.run(buildPair("Life", "42"));
    }

}
