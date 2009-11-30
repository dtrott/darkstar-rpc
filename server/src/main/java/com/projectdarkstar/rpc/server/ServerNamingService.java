package com.projectdarkstar.rpc.server;

import com.projectdarkstar.rpc.CoreRpc.MetaService;
import com.projectdarkstar.rpc.CoreRpc.ServiceDetails;
import com.projectdarkstar.rpc.common.NamingService;

import java.io.Serializable;

public interface ServerNamingService extends NamingService, Serializable, MetaService.Interface {

    public ServiceDetails getServiceDetails(String fullName);
}
