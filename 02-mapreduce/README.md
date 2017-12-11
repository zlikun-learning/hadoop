`Windows`下提交`MapReduce`到`YARN`中运行，客户端`Hadoop`版本应尽量与服务端版本一致(不一致可能会有问题，未验证)，另外服务端按下述配置进行配置(经测试可用，踩了太多坑了T_T)

服务端主机名`zlikun`配置为内网IP，不要使用(127.0.0.1 / 127.0.1.1 / localhost 等)，原因后续研究

`Windows`下开发`MapReduce`应用时，需要将 <https://github.com/steveloughran/winutils> 中的相应文件，复制到 `$HADOOP_HOME/bin` 目录下

- core-site.xml 
```
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://zlikun:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/home/zlikun/.tmp</value>
    </property>
    <property>
        <name>dfs.permissions</name>
        <value>false</value>
    </property>
</configuration>
```
- hdfs-site.xml 
```
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.rpc-address</name>
        <value>zlikun:9000</value>
    </property>
    <property>
        <name>dfs.namenode.rpc-bind-host</name>
        <value>0.0.0.0</value>
    </property>
</configuration>
```
- yarn-site.xml 
```
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
</configuration>
```
- mapred-site.xml
```
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
</configuration>
```