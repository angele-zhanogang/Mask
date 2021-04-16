package com.angle.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.net.InetSocketAddress;
import java.util.logging.Logger;

public class demo2 {
    private static final Logger Log = Logger.getLogger(demo2.class.getName());
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        YarnConfiguration yarnConf = new YarnConfiguration(conf);

        InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(YarnConfiguration.RM_ADDRESS,
                YarnConfiguration.DEFAULT_RM_ADDRESS));
        Log.info("connecting to resourcemanager at " +rmAddress);

        Configuration appsManagerServerConf = new Configuration(conf);


//        addToLocalResources

    }
}
