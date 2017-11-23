# hadoop

http://hadoop.apache.org/

#### Windows
```
在Windows下测试Hadoop时，除Hadoop自身外还依赖`winutils.exe`和`hadoop.dll`等组件，
下载地址：https://github.com/steveloughran/winutils

将`winutils.exe`复制到Hadoop的bin目录下，将`hadoop.dll`复制到C:/Windows/System32目录下
配置环境变量：HADOOP_HOME，指向Hadoop目录，可能要重启IDE
```