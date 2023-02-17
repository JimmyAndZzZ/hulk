# hulk

### 主要特性

- 基于Mysql协议，可以通过navicat、jdbc等连接方式
- 支持用SQL查询非结构化和结构化数据库，例如Mysql、Oracle、Clickhouse、Excel、elasticsearch
- 支持不同数据源之间表关联查，目前支持inner join和left join
- 基于calcite进行SQL解析，并对SQL进行扩展，支持自定义扩展操作
- 基于aviator表达式进行自定义函数扩展
- 支持分库分表、权限设置(可到字段级别)

### 安装使用

----

#### 安装

下载tar包后解压缩，对conf目录下booster.xml进行配置

----

#### 配置数据源

可参考代码中的配置进行修改

----

#### 启动停止

通过bin目录下的脚本进行启停(linux环境下需要set ff=unix)

### 配置详解

----

#### 系统参数

```html

<system>
    <!--启动端口号-->
    <property name="port">1112</property>
    <!--页大小，默认200-->
    <property name="pageSize">1000</property>
    <!--缓存有效时间，默认30，单位：分-->
    <property name="defaultExpire">30</property>
    <!--文件存储根路径-->
    <property name="fileStorePath">/tmp/</property>
</system>
```

----

#### 数据源配置(其他数据源参考样例xml)

```html

<datasourceList>
    <datasource type="mysql">
        <!--数据源名称，需与schema保持一致-->
        <name>test1</name>
        <password><![CDATA[123456]]></password>
        <schema>test1</schema>
        <clusterName></clusterName>
        <url>
            <![CDATA[jdbc:mysql://localhost:3306/test1?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8]]>
        </url>
        <username>root</username>
    </datasource>
</datasourceList>
```

----

#### 数据实例配置

```html

<example name="test1" title="测试库">
    <!--写数据源-->
    <write>test1</write>
    <!--读数据源-->
    <read>test3</read>
    <!--指定主键生成方式，配置后可以根据主键以及主键生成方式进行操作，生成策略为auto支持预处理insert后返回(仅对于mysql)，其他生成策略:snowflake、uid-->
    <table tableName="test_int" priKeyName="id" priKeyStrategy="auto" isNeedReturnKey="true"></table>
</example>
```

----

#### 分库分表配置

```html
 <!--partitionColumn分库分表字段，mod代表散列策略，默认为hash-->
<partition partitionColumn="trace_id" mod="long-mod" dsName="test1" priKeyColumn="id" table="example_par"
           type="mysql">
    <!--prefix：代表表名前缀，range支持(和[的表达方式-->
    <table prefix="example_par_" range="[0,10)" dsName="test1">
        <!--可以直接指定表名-->
        <name>example_par_1000</name>
    </table>
    <table prefix="example_par_" range="[0,10)" dsName="test_report"></table>
</partition>
```

----

#### 权限配置

```html

<users>
    <!--ADMINISTRATOR代表超级管理员-->
    <user role="ADMINISTRATOR">
        <username>root</username>
        <password><![CDATA[123456]]></password>
    </user>

    <!--ORDINARY_USER普通用户需要指定schema-->
    <user role="ORDINARY_USER">
        <username>dev</username>
        <password><![CDATA[123456]]></password>
        <schema dsName="test1" isAllAllow="true">
            <!--指定表的操作方式以及insert，update时候需要过滤的字段名-->
            <table table="test00" filterFields="inputer" dmlAllowMethods="INSERT"></table>
            <table table="test_int" filterFields="t_2" dmlAllowMethods="INSERT,UPDATE,DELETE"></table>
        </schema>
    </user>

    <!--GUEST，游客用户没有dml权限-->
    <user role="GUEST">
        <username>guest</username>
        <password><![CDATA[123456]]></password>
        <schema dsName="test1"/>
    </user>
</users>
```

### 自定义SQL

----

#### cacheQuery

```sql
CACHE
from 'test1' EXPIRE '10'  sql ' SELECT
	t1.f1,
	t1.f2,
	t1.f3,
	t2.f4 
FROM
	test1.table_1 AS t1
	INNER JOIN test3.table_2 AS t2 ON t1.f1 = t2.f1 
	AND t1.create_date > ''2022-12-30 16:29:41:00''
	where t1.f2=''N0000342023CORLEB01'' or t2.f3=''111'''    
```

> EXPIRE代表过期时间，默认为系统配置，单位分钟

----

#### flush

```sql
flush
to 'test_excel' index 'table3' MAPPER '' as ' SELECT
	t1.f1,
	t1.f2,
	t1.f3,
	t2.f4 
FROM
	test1.table_1 AS t1
	INNER JOIN test3.table_2 AS t2 ON t1.f1 = t2.f1 
	AND t1.create_date > ''2022-12-30 16:29:41:00''
	where t1.f2=''N0000342023CORLEB01'' or t2.f3=''123'''
```

> to代表目标数据源，index代表目标表，mapper代表字段映射以及插入方式，可以参考源码com.jimmy.hulk.actuator.sql.Flush

----

#### native

```sql
NATIVE
EXECUTE|QUERY 'test1' SQL 'select 1 from 1'      
```

> EXECUTE|QUERY 可选，execute执行ddl或者dml操作，query代表原生SQL查询
----

### 补充

目前平台刚通过测试阶段，欢迎大家一起补充。

#### 未完成点

- 目前navicat支持查看，但dml操作提示主键未找到后期会修复
- 目前不支持group by、exists复杂性sql
- 聚合函数目前只支持count,max,min,avg
- alter语句目前不支持