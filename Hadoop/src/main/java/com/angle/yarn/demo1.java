package com.angle.yarn;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class demo1 {

    private static final Logger log = Logger.getLogger(demo1.class.getName());

    public static void main(String[] args) {
//        ApplicationId applicationId = ApplicationId.newInstance(1000L, 233222332);
//        GetNewApplicationResponse response = GetNewApplicationResponse.newInstance(applicationId, Resource.newInstance(512, 1), Resource.newInstance(1024, 1));
//
//        ContainerLaunchContext.newInstance()
//
//        ApplicationSubmissionContext.newInstance(applicationId,"","", Priority.UNDEFINED,)
//        new YarnClientApplication()

//        //开启客户端连接
//        YarnConfiguration conf = new YarnConfiguration();
//        //加载yarn配置,配置的地址如果是host,需配置ip映射
//        conf.addResource(new Path(""));
//        //YarnClient负责用途ResourceManager通信,协议是ApplicationClientProtocol
//        YarnClient yarnClient = YarnClient.createYarnClient();
//
//        yarnClient.init(conf);
//
//        yarnClient.start();
//
//        //创建客户端application
//        try {
//            YarnClientApplication app = yarnClient.createApplication();
//            ApplicationSubmissionContext context = app.getApplicationSubmissionContext();
//            ApplicationId appId = context.getApplicationId();
//            System.out.println(appId);
//
//            context.setKeepContainersAcrossApplicationAttempts(true);
//            context.setApplicationName("test");
//
//            //为应用程序主机设置本地资源
//            //根据需要设置本地文件或者档案
//            //在这种情况下,应用程序主机的jar问价你是本地资源的一部分
//            Map<String, LocalResource> localResources  = new HashMap<>();
//
//            log.info("从本地文件系统复制 App Master jar 并添加到本地环境");
//            //将应用程序主jar复制到问价系统
//            //创建一个本地资源以指向木目标jar路径
//
//            FileSystem fs = FileSystem.get(conf);
//
////            addToLocalResources
//
//
//
//        } catch (YarnException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//

        if(1 > 2)
            System.out.println("aaaaaaa");

    }
}
