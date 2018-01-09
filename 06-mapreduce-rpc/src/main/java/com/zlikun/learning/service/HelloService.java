package com.zlikun.learning.service;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/1/9 21:37
 */
public interface HelloService {

    long versionID = 1L ;

    String say(String message) ;

}
