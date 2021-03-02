package com.angle.utils;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class EsUtils {
    private static TransportClient client;

    private static void init() throws UnknownHostException {
        String clusterName = Resources.getProperty("es.cluster.name");
        String ip1 = Resources.getProperty("es.Ip1");
        String ip2 = Resources.getProperty("es.Ip2");
        String ip3 = Resources.getProperty("es.Ip3");
        int port = Integer.parseInt(Resources.getProperty("es.port"));
        Settings settings = Settings.builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", "true") //自动发现es其他服务器
                .build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName(ip1), port))
                .addTransportAddress(new TransportAddress(InetAddress.getByName(ip2), port))
                .addTransportAddress(new TransportAddress(InetAddress.getByName(ip3), port))
        ;
    }

    public static TransportClient getClient() throws UnknownHostException {
        if(client ==null){
            init();
        }
        return client;
    }
}
