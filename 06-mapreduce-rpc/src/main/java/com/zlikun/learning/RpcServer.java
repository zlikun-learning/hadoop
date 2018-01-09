package com.zlikun.learning;

import com.zlikun.learning.service.HelloService;
import com.zlikun.learning.service.HelloServiceImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/1/9 21:36
 */
public class RpcServer {

    public static void main(String[] args) throws IOException {

        // 创建RPC服务
        RPC.Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(7744)
                .setProtocol(HelloService.class)
                .setInstance(new HelloServiceImpl())
                .build();

        // 启动RPC服务
        server.start();

    }

}
