# XiaoFeng.Redis

![fayelf](https://user-images.githubusercontent.com/16105174/197918392-29d40971-a8a2-4be4-ac17-323f1d0bed82.png)

![GitHub top language](https://img.shields.io/github/languages/top/zhuovi/xiaofeng.redis?logo=github)
![GitHub License](https://img.shields.io/github/license/zhuovi/xiaofeng.redis?logo=github)
![Nuget Downloads](https://img.shields.io/nuget/dt/xiaofeng.redis?logo=nuget)
![Nuget](https://img.shields.io/nuget/v/xiaofeng.redis?logo=nuget)
![Nuget (with prereleases)](https://img.shields.io/nuget/vpre/xiaofeng.redis?label=dev%20nuget&logo=nuget)

Nuget：XiaoFeng.Redis

| QQ群号 | QQ群 | 公众号 |
| :----:| :----: | :----: |
| 748408911  | ![QQ 群](https://user-images.githubusercontent.com/16105174/198058269-0ea5928c-a2fc-4049-86da-cca2249229ae.png) | ![畅聊了个科技](https://user-images.githubusercontent.com/16105174/198059698-adbf29c3-60c2-4c76-b894-21793b40cf34.jpg) |

源码： https://github.com/zhuovi/xiaofeng.redis

教程： https://www.yuque.com/fayelf/xiaofeng

Redis中间件,支持.NET框架、.NET内核和.NET标准库,一种非常方便操作的客户端工具。实现了Hash,Key,String,ZSet,Stream,Log,订阅发布,线程池功能。

## XiaoFeng.Redis

XiaoFeng.Redis generator with [XiaoFeng.Redis](https://github.com/zhuovi/XiaoFeng.Redis).

## Install

.NET CLI

```
$ dotnet add package XiaoFeng.Redis --version 2.0.1
```

Package Manager

```
PM> Install-Package XiaoFeng.Redis --Version 2.0.1
```

PackageReference

```
<PackageReference Include="XiaoFeng.Redis" Version="2.0.1" />
```

Paket CLI

```
> paket add XiaoFeng.Redis --version 2.0.1
```

Script & Interactive

```
> #r "nuget: XiaoFeng.Redis, 2.0.1"
```

Cake

```
// Install XiaoFeng.Redis as a Cake Addin
#addin nuget:?package=XiaoFeng.Redis&version=2.0.1

// Install XiaoFeng.Redis as a Cake Tool
#tool nuget:?package=XiaoFeng.Redis&version=2.0.1
```

# XiaoFeng 类库包含库
| 命名空间 | 所属类库 | 开源状态 | 说明 | 包含功能 |
| :----| :---- | :---- | :----: | :---- |
| XiaoFeng.Prototype | XiaoFeng.Core | :white_check_mark: | 扩展库 | ToCase 类型转换<br/>ToTimestamp,ToTimestamps 时间转时间戳<br/>GetBasePath 获取文件绝对路径,支持Linux,Windows<br/>GetFileName 获取文件名称<br/>GetMatch,GetMatches,GetMatchs,IsMatch,ReplacePatten,RemovePattern 正则表达式操作<br/> |
| XiaoFeng.Net | XiaoFeng.Net | :white_check_mark: | 网络库 | XiaoFeng网络库，封装了Socket客户端，服务端（Socket,WebSocket），根据当前库可轻松实现订阅，发布等功能。|
| XiaoFeng.Http | XiaoFeng.Core | :white_check_mark: | 模拟请求库 | 模拟网络请求 |
| XiaoFeng.Data | XiaoFeng.Core | :white_check_mark: | 数据库操作库 | 支持SQLSERVER,MYSQL,ORACLE,达梦,SQLITE,ACCESS,OLEDB,ODBC等数十种数据库 |
| XiaoFeng.Cache | XiaoFeng.Core | :white_check_mark: | 缓存库 |  内存缓存,Redis,MemcachedCache,MemoryCache,FileCache缓存 |
| XiaoFeng.Config | XiaoFeng.Core | :white_check_mark: | 配置文件库 | 通过创建模型自动生成配置文件，可为xml,json,ini文件格式 |
| XiaoFeng.Cryptography | XiaoFeng.Core | :white_check_mark: | 加密算法库 | AES,DES,RSA,MD5,DES3,SHA,HMAC,RC4加密算法 |
| XiaoFeng.Excel | XiaoFeng.Excel | :white_check_mark: | Excel操作库 | Excel操作，创建excel,编辑excel,读取excel内容，边框，字体，样式等功能  |
| XiaoFeng.Ftp | XiaoFeng.Ftp | :white_check_mark: | FTP请求库 | FTP客户端 |
| XiaoFeng.IO | XiaoFeng.Core | :white_check_mark: | 文件操作库 | 文件读写操作 |
| XiaoFeng.Json | XiaoFeng.Core | :white_check_mark: | Json序列化，反序列化库 | Json序列化，反序列化库 |
| XiaoFeng.Xml | XiaoFeng.Core | :white_check_mark: | Xml序列化，反序列化库 | Xml序列化，反序列化库 |
| XiaoFeng.Log | XiaoFeng.Core | :white_check_mark: | 日志库 | 写日志文件,数据库 |
| XiaoFeng.Memcached | XiaoFeng.Memcached | :white_check_mark: | Memcached缓存库 | Memcached中间件,支持.NET框架、.NET内核和.NET标准库,一种非常方便操作的客户端工具。实现了Set,Add,Replace,PrePend,Append,Cas,Get,Gets,Gat,Gats,Delete,Touch,Stats,Stats Items,Stats Slabs,Stats Sizes,Flush_All,Increment,Decrement,线程池功能。|
| XiaoFeng.Redis | XiaoFeng.Redis | :white_check_mark: | Redis缓存库 | Redis中间件,支持.NET框架、.NET内核和.NET标准库,一种非常方便操作的客户端工具。实现了Hash,Key,String,ZSet,Stream,Log,List,订阅发布,线程池功能; |
| XiaoFeng.Threading | XiaoFeng.Core | :white_check_mark: | 线程库 | 线程任务,线程队列 |
| XiaoFeng.Mvc | XiaoFeng.Mvc | :x: | 低代码WEB开发框架 | .net core 基础类，快速开发CMS框架，真正的低代码平台，自带角色权限，WebAPI平台，后台管理，可托管到服务运行命令为:应用.exe install 服务名 服务说明,命令还有 delete 删除 start 启动  stop 停止。 |
| XiaoFeng.Proxy | XiaoFeng.Proxy | :white_check_mark: | 代理库 | 开发中 |
| XiaoFeng.TDengine | XiaoFeng.TDengine | :white_check_mark: | TDengine 客户端 | 开发中 |
| XiaoFeng.GB28181 | XiaoFeng.GB28181 | :white_check_mark: | 视频监控库，SIP类库，GB28181协议 | 开发中 |
| XiaoFeng.Onvif | XiaoFeng.Onvif | :white_check_mark: | 视频监控库Onvif协议 | XiaoFeng.Onvif 基于.NET平台使用C#封装Onvif常用接口、设备、媒体、云台等功能， 拒绝WCF服务引用动态代理生成wsdl类文件 ， 使用原生XML扩展标记语言封装参数，所有的数据流向都可控。 |


# XiaoFeng.Redis

Redis提供了友好的访问API。

## 基本使用方法

Redis连接串 

```csharp
redis://redisname:7092734@127.0.0.1:6379/0?ConnectionTimeout=3000&ReadTimeout=3000&SendTimeout=3000&pool=3
[<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
|----------|---|-----------|-----------|------|------|------------|-----------------------|
| protocol |   | username  | password  | host | port |  database  |  params               |

```
redisname   用户名

7092734	    密码

127.0.0.1	主机

6379		端口

0			0库

ConnectionTimeout	连接超时时长

ReadTimeout		    读取数据超时时长

SendTimeout		    发送数据超时时长

pool			    连接池中连接数量

最小的连接串是：redis://127.0.0.1

实例化一个redis对象

```csharp
var redis = new XiaoFeng.Redis.RedisClient("redis://7092734@127.0.0.1:6379/0");
```

## KEY的基本操作

```csharp

//删除连接串中默认库中 key为 a,b的key
var r1 = await redis.DelKeyAsync("a", "b");
//删除1库中key为 a,b的key
var r2 = await redis.DelKeyAsync(1, "a", "b");
//删除0库中key为a,并返回a的值
var r3 = await redis.GetDelKeyAsync("a", 0);
//序列化key
var r4 = await redis.DumpKeyAsync("a");
//是否存在key
var r5 = await redis.ExistsKeyAsync("a");
//设置过期时间 秒
var r6 = await redis.SetKeyExpireSecondsAsync("a", 100);
//设置过期时间 毫秒
var r7 = await redis.SetKeyExpireMillisecondsAsync("a", 100000);
//设置过期时长    时间戳     秒
var r8 = await redis.SetKeyExpireSecondsTimestampAsync("a", 123456);
//设置过期时长    时间戳     毫秒
var r9 = await redis.SetKeyExpireMillisecondsTimestampAsync("a", 123456);
//重命名key a 到 b
var r10 = await redis.RenameKeyAsync("a", "b");
//重命名key a 到 b 并且b不存在
var r11 = await redis.RenameKeyNoExistsAsync("a", "b");
//移动key 从库 0 至 库 1
var r12 = await redis.MoveKeyAsync("a", 1, 0);
//移除过期时间
var r13 = await redis.RemoveKeyExpireAsync("a");
//获取剩余时间    秒
var r14 = await redis.GetKeyExpireSecondsAsync("a");
//获取剩余时间    毫秒
var r15 = await redis.GetKeyExpireMillisecondsAsync("a");
//从当前数据库随机获取一个key
var r16 = await redis.GetKeyRandomAsync(0);
//获取key值 类型
var r17 = await redis.GetKeyTypeAsync("a");
//复制一个key
var r18 = await redis.CopyKeyAsync("a", "b");
//查找 key 支持 * 和? * 代表好多个字符  ?代表一个字符
var r19 = await redis.GetKeysAsync("*a");
//获取当前库 key 的数量
var r20 = await redis.GetDbKeySizeAsync();
//查询当前库中符合模式的key
var r21 = await redis.GetKeysAsync("*a");
//获取key为a的值类型
var r22 = await redis.GetKeyTypeAsync("a");
//删除当前库key为a b的数据
var r23 = await redis.DelKeyAsync("a", "b");
//删除当前库所有key
var r24 = await redis.DelDbKeysAsync();
//删除所有库的所有key
var r25 = await redis.DelAllKeysAsync();

```

## 字符串类型操作

```csharp

//设置key a 的值 为 "abc"
var s1 = await redis.SetStringAsync("a", "abc");
//批量设置 key 和 值
var s2 = await redis.SetStringAsync(new Dictionary<string, object>
{
    { "a","abc" },
    { "b",DateTime.Now },
    { "c",11110 },
    { "d",123.23 },
    { "e",Guid.NewGuid() },
    { "f",new List<string>{"a","b","c","d"} }
    });
//设置key a 的 值 为 b 并且 a不存在时才成功
var s3 = await redis.SetStringNoExistsAsync("a", "b");
//获取key a ,b 的值
var s4 = await redis.GetStringAsync("a", "b");
//获取key a 的值 的长度
var s5 = await redis.GetStringLengthAsync("a");
//设置 key a 的值 为 111 并返回 原来的值
var s6 = await redis.GetSetStringAsync("a", 111);
//自动加1
var s7 = await redis.StringIncrementAsync("a");
//自动减1
var s8 = await redis.StringDecrementAsync("a");
```

## Hash操作

```csharp

//设置hash key为a 字段名为 b 值为 c
var h1 = await redis.SetHashAsync("a", "b", "c");
//批量设置has key为a
var h2 = await redis.SetHashAsync("a", new Dictionary<string, object>
{
    { "a","abc" },
    { "b",DateTime.Now },
    { "c",11110 },
    { "d",123.23 },
    { "e",Guid.NewGuid() },
    { "f",new List<string>{"a","b","c","d"} }
});
//设置hash key 为a 字段名为b 值为 c 只有字段名b不存在时才设置成功
var h3 = await redis.SetHashNoExistsAsync("a", "b", "c");
//删除hash key 为 a 中的字段名为 "b","c"的键值
var h4 = await redis.DelHashAsync("a", "b", "c");
//查询hash key 为 a 中的字段为 "b" 是否存在
var h5 = await redis.ExistsHashAsync("a", "b");
//设置hash key 为 a 中的字段为 "b" 的值增加2 如果减则输入负数即可
var h6 = await redis.HashIncrementAsync("a", "b", 2);
```

## List操作

```csharp

//设置List key 为 a  值为 "b","c","d"
var l1 = await redis.SetListItemAsync("a", "b", "c", "d");
//设置List key 为 a 索引为2 的值 为 111
var l2 = await redis.SetListItemAsync("a", 2, 111);
//在List Key 为 a 的最前边加入 "b","c","d"值
var l3 = await redis.SetListItemBeforeAsync("a", "b", "c", "d");
//将111插入到已存在key 为a的List中最前边 不存在则不插入
var l4 = await redis.SetListItemBeforeExistsAsync("a", 111);
//将"c"插入到已存在key 为a的List中最后边 不存在则不插入
var l5 = await redis.SetListItemExistsAsync("a", "c");
//从 List key为a中从头到尾搜索3个值为"c"的数据 然后移除 如果想从尾到头搜索则变成负数即可 如 -3
var l6 = await redis.DelListItemAsync("a", "c", 3);
//从 List key为a中移除索引从2到5的数据
var l7 = await redis.DelListItemAsync("a", 2, 5);
//获取List中key为a的值数量
var l8 = await redis.GetListCountAsync("a");
//移出并获取列表的第一个元素
var l9 = await redis.GetListFirstItemAsync("a");
//通过索引获取列表中的元素
var l10 = await redis.GetListItemAsync("a", 2);
//获取List中key为a的 索引从2到5的值
var l11 = await redis.GetListItemsAsync("a", 2, 5);
//List 从key为a的尾部取出数据并插入到key为b的头部
var l12 = await redis.GetListLastItemToOtherListFirstAsync("a", "b");
```

## Set操作

```csharp

//向Set中key为"a"的插入"b","c"成员
var set1 = await redis.SetSetMemberAsync("a", "b", "c");
//获取Set中key为a的值数量
var set2 = await redis.GetSetCountAsync("a");
//获取List中key 为a和b 值的差集
var set3 = await redis.GetSetDiffAsync("a", "b");
//获取List中key 为a和b 值的差集 并把差集存到key为c中
var set4 = await redis.GetSetDiffStoreAsync("a", "c", "b");
//获取List中key 为a和b 值的交集
var set5 = await redis.GetSetInterAsync("a", "b");
//获取List中key 为a和b 值的交集 并把差集存到key为c中
var set6 = await redis.GetSetInterStoreAsync("a", "c", "b");
//获取Set中 key为a的所有成员
var set7 = await redis.GetSetMemberListAsync("a");
//移除并返回集合中的一个或多个随机元素
var set8 =await redis.GetSetPopAsync("a");
//获取集合中一个或多个随机数
var set9 = await redis.GetSetRandomMemberAsync("a");
//删除集合中的b c 成员
var set10 = await redis.DelSetMemberAsync("a", "b", "c");
//判断成员b是否是a集合的成员
var set11 = await redis.ExistsSetMemberAsync("a", "b");
//获取集合 a b的并集
var set12 = await redis.GetSetUnionAsync("a", "b");
//获取集合 a b的并集 并存到集合c中
var set13 = await redis.GetSetUnionStoreAsync("a", "c", "b");
```

## Sorted set 操作 和Set 操作基本相似

```csharp

//在这里就不再举例

```


# 作者介绍

* 网址 : http://www.fayelf.com
* QQ : 7092734
* EMail : jacky@fayelf.com