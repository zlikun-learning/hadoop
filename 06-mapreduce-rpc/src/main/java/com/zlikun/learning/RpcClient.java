package com.zlikun.learning;

import com.zlikun.learning.service.HelloService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/1/9 21:39
 */
public class RpcClient {

    public static void main(String[] args) throws IOException {

        // 获取服务代理
        HelloService proxy = RPC.getProxy(HelloService.class, 1L, new InetSocketAddress("localhost", 7744), new Configuration());

        // 通过代理调用服务
        String rt = proxy.say("hello");
        System.out.println(rt);

    }

}
