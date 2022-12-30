using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using XiaoFeng.Collections;
using XiaoFeng.Data;
using XiaoFeng.Threading;

/****************************************************************
*  Copyright © (2021) www.fayelf.com All Rights Reserved.       *
*  Author : jacky                                               *
*  QQ : 7092734                                                 *
*  Email : jacky@fayelf.com                                     *
*  Site : www.fayelf.com                                        *
*  Create Time : 2021-06-14 14:33:18                            *
*  Version : v 1.1.0                                            *
*  CLR Version : 4.0.30319.42000                                *
*****************************************************************/
namespace XiaoFeng.Redis
{
    /// <summary>
    /// Redis 客户端操作类
    /// v 1.1.0
    /// 修改提取数据用正则改为字符提取 更加精确
    /// </summary>
    public partial class RedisClient : RedisBaseClient,IRedisClient
    {
        #region 构造器
        /// <summary>
        /// 设置连接串
        /// </summary>
        /// <param name="host">主机</param>
        /// <param name="port">端口</param>
        /// <param name="password">密码</param>
        /// <param name="MaxPool">应用池数量</param>
        public RedisClient(string host = "127.0.0.1", int port = 6379, string password = "", int? MaxPool = null)
        {
            this.ConnConfig = new RedisConfig
            {
                ProviderType = DbProviderType.Redis,
                Host = host,
                Port = port,
                Password = password,
                MaxPool = MaxPool ?? 0,
                IsPool = MaxPool.HasValue
            };
        }
        /// <summary>
        /// 设置连接串
        /// </summary>
        /// <param name="host">主机</param>
        /// <param name="password">密码</param>
        /// <param name="port">端口</param>
        /// <param name="MaxPool">应用池数量</param>
        public RedisClient(string host, string password, int port = 6379, int? MaxPool = null) : this(host, port, password, MaxPool)
        { }
        /// <summary>
        /// 设置连接串
        /// </summary>
        /// <param name="connectionString">连接串</param>
        public RedisClient(string connectionString)
        {
            this.ConnConfig = new RedisConfig(connectionString);
        }
        /// <summary>
        /// 设置配置
        /// </summary>
        /// <param name="config">配置</param>
        public RedisClient(ConnectionConfig config)  { this.ConnConfig = config.ToRedisConfig(); }
        /// <summary>
        /// 设置配置
        /// </summary>
        /// <param name="config">Redis配置</param>
        public RedisClient(RedisConfig config) { this.ConnConfig = config; }
        #endregion

        #region 方法

        #endregion
    }
}