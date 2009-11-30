package com.projectdarkstar.rpc.server;

import com.sun.sgs.app.AppContext;
import com.sun.sgs.app.DataManager;

public class ServerNamingServiceFactory {

    public static final String DEFAULT_BINDING = "NamingService";

    public static void init() {
        final DataManager dataManager = AppContext.getDataManager();
        dataManager.setBinding(DEFAULT_BINDING, new ServerNamingServiceImpl());
    }

    public static ServerNamingService getNamingService() {
        final DataManager dataManager = AppContext.getDataManager();

        return new ServerNamingServiceWrapper(dataManager.createReference((ServerNamingService) dataManager.getBinding(DEFAULT_BINDING)));
    }
}
