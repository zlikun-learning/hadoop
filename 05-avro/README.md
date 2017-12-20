# Avro

- <http://avro.apache.org/docs/current/>
- <http://avro.apache.org/docs/current/gettingstartedjava.html>

#### 数据类型与模式
基本数据类型

| 类型 | 描述 | 模式示例 |
| --- | --- | --- |
| null | 空值 | "null" |
| boolean | 二进制值 | "boolean" |
| int | 32位整带符号整数 | "int" |
| long | 64位整带符号整数 | "long" |
| float | 单精度(32位)IEEE 754浮点数 | "float" |
| double | 双精度(64位)IEEE 754浮点数 | "double" |
| bytes | 8位无符号字节序列 | "bytes" |
| string | Unicode 字符序列 | "string" |

复杂类型
- array
> 一个排过序的对象集合，特定数组中的所有对象必须模式相同
```
{
  "type":"array",
  "items":"long"
}
```
- map
> 未排过序的键值对，键必须是字符串，值可以是任意类型，但一个特定的map中所有值类型必须模式相同
```
{
  "type":"map",
  "values":"string"
}
```
- record
> 一个任意类型的命名字段集合
```
{
  "type": "record",
  "name": "WeatherRecord",
  "doc": "A weather reading.",
  "fields": [
    {"name": "year", "type": "int"},
    {"name": "temperature", "type": "int"},
    {"name": "stationId", "type": ["string", "null"]}
  ]
}
```
- enum
> 一个命名的值集合
```
{
  "type":"enum",
  "name":"Cutlery",
  "doc":"A eating utensil.",
  "symbols":["KNIFE", "FORK", "SPOON"]
}
```
- fixed
> 一组固定数量的8位无符号字节
```
{
  "type":"fixed",
  "name":"Md5Hash",
  "size":16
}
```
- union
> 模式的并集，并集可用JSON数组表示，其中每个元素为一个模式，并集表示的数据必须与其内的某个模式匹配
```
[
  "null",
  "string",
  {"type":"map", "values":"string"}
]
```

与Java类型的映射

| Avro类型 | Java通用映射 | Java特殊映射 | Java自反映射 |
| --- | --- | --- | --- |
| null | null |||
| boolean | boolean |||
| int | int || short或int |
| long | long |||
| float | float |||
| double | double |||
| bytes | java.nio.bytebuffer || 字节数组 |
| string | org.apache.avro.util.utf8 || java.lang.String |
| array | org.apache.avro.generic.GenericArray || 数组或java.util.Collection |
| map | java.util.map |||
| record | org.apache.avro.generic.genericrecord | 生成实现org.apache.avro.specific.SpecificRecord类的实现 | 具有零参构造函数的任意用户类，继承了所有不传递的实例字段 |
| enum | java.lang.String | 生成Java enum类型 | 任意Java enum类型 |
| fixed | org.apache.avro.generic.genericfixed | 生成实现org.apache.avro.specific.SpecificFixed的类 | org.apache.avro.generic.genericFixed |
| union | java.lang.Object |||

#### 特定API (Java为例)
> 通过 `avro-maven-plugin` 插件，可以实现基于 `*.avsc` 文件生成Java类，该类包含 `Schema` 信息，所以可以直接基于该类实现序列化与反序列化，通过 `mvn compile` 即可生成  
> 非 Maven 工程，可以通过 Ant 任务来实现 (略)，或者 Avro 的命令行工具来为一个模式生 JAVA 代码

- avro-maven-plugin
```
<plugin>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro-maven-plugin</artifactId>
    <version>1.8.2</version>
    <executions>
        <execution>
            <phase>generate-sources</phase>
            <goals>
                <goal>schema</goal>
            </goals>
            <configuration>
                <sourceDirectory>${project.basedir}/src/main/avro/</sourceDirectory>
                <outputDirectory>${project.basedir}/src/main/java/</outputDirectory>
            </configuration>
        </execution>
    </executions>
</plugin>
```

- avro-tools
```
# 查看帮助
$ java -jar avro-tools-1.8.2.jar
Version 1.8.2
 of Apache Avro
Copyright 2010-2015 The Apache Software Foundation

This product includes software developed at
The Apache Software Foundation (http://www.apache.org/).
----------------
Available tools:
          cat  extracts samples from files
      compile  Generates Java code for the given schema.
       concat  Concatenates avro files without re-compressing.
   fragtojson  Renders a binary-encoded Avro datum as JSON.
     fromjson  Reads JSON records and writes an Avro data file.
     fromtext  Imports a text file into an avro data file.
      getmeta  Prints out the metadata of an Avro data file.
    getschema  Prints out schema of an Avro data file.
          idl  Generates a JSON schema from an Avro IDL file
 idl2schemata  Extract JSON schemata of the types from an Avro IDL file
       induce  Induce schema/protocol from Java class/interface via reflection.
   jsontofrag  Renders a JSON-encoded Avro datum as binary.
       random  Creates a file with randomly generated instances of a schema.
      recodec  Alters the codec of a data file.
       repair  Recovers data from a corrupt Avro Data file
  rpcprotocol  Output the protocol of a RPC service
   rpcreceive  Opens an RPC Server and listens for one message.
      rpcsend  Sends a single RPC message.
       tether  Run a tethered mapreduce job.
       tojson  Dumps an Avro data file as JSON, record per line or pretty.
       totext  Converts an Avro data file to a text file.
     totrevni  Converts an Avro data file to a Trevni file.
  trevni_meta  Dumps a Trevni file's metadata as JSON.
trevni_random  Create a Trevni file filled with random instances of a schema.
trevni_tojson  Dumps a Trevni file as JSON.
```