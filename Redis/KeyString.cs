using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

/****************************************************************
*  Copyright © (2021) www.fayelf.com All Rights Reserved.       *
*  Author : jacky                                               *
*  QQ : 7092734                                                 *
*  Email : jacky@fayelf.com                                     *
*  Site : www.fayelf.com                                        *
*  Create Time : 2021-07-07 11:01:29                            *
*  Version : v 1.0.0                                            *
*  CLR Version : 4.0.30319.42000                                *
*****************************************************************/
namespace XiaoFeng.Redis
{
    /// <summary>
    /// KEY 操作
    /// </summary>
    public partial class RedisClient : RedisBaseClient, IRedisClient
    {
        #region 字符串(String)

        #region 设置字符串
        /// <summary>
        /// 设置字符串
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="key">key</param>
        /// <param name="value">值</param>
        /// <param name="timeSpan">过期时间</param>
        /// <param name="dbNum">数据库</param>
        /// <returns>是否设置成功</returns>
        public Boolean SetString<T>(string key, T value, TimeSpan? timeSpan = null, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return false;
            var args = new List<object>() { key, this.GetValue(value) };
            if (timeSpan != null) args.Insert(1, timeSpan.Value.TotalSeconds);
            return this.Execute(timeSpan != null ? CommandType.SETEX : CommandType.SET, dbNum, result => result.OK, args.ToArray());
        }
        /// <summary>
        /// 设置字符串 异步
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="key">key</param>
        /// <param name="value">值</param>
        /// <param name="timeSpan">过期时间</param>
        /// <param name="dbNum">库索引</param>
        /// <returns>是否设置成功</returns>
        public async Task<Boolean> SetStringAsync<T>(string key, T value, TimeSpan? timeSpan = null, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return false;
            var args = new List<object>() { key, this.GetValue(value) };
            if (timeSpan != null) args.Insert(1, timeSpan.Value.TotalSeconds);
            return await this.ExecuteAsync(timeSpan != null ? CommandType.SETEX : CommandType.SET, dbNum, async result => await Task.FromResult(result.OK), args.ToArray());
        }
        /// <summary>
        /// 批量设置值
        /// </summary>
        /// <param name="values">key值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public Boolean SetString(Dictionary<string, object> values, int? dbNum = null)
        {
            if (values == null || values.Count == 0) return false;
            var list = new List<object>();
            values.Each(v =>
            {
                list.Add(v.Key);
                list.Add(this.GetValue(v.Value));
            });
            return this.Execute(CommandType.MSET, dbNum, result => result.OK, list.ToArray());
        }
        /// <summary>
        /// 批量设置值 异步
        /// </summary>
        /// <param name="values">key值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public async Task<Boolean> SetStringAsync(Dictionary<string, object> values, int? dbNum = null)
        {
            if (values == null || values.Count == 0) return false;
            var list = new List<object>();
            values.Each(v =>
            {
                list.Add(v.Key);
                list.Add(this.GetValue(v.Value));
            });
            return await this.ExecuteAsync(CommandType.MSET, dbNum, async result => await Task.FromResult(result.OK), list.ToArray());
        }
        /// <summary>
        /// 设置字符串 key不存在时
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="key">key</param>
        /// <param name="value">值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns>是否设置成功</returns>
        public Boolean SetStringNoExists<T>(string key, T value, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return false;
            return this.Execute(CommandType.SETNX, dbNum, result => result.OK && result.Value.ToInt() > 0, key, this.GetValue(value));
        }
        /// <summary>
        /// 设置字符串 key不存在时 异步
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="key">key</param>
        /// <param name="value">值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns>是否设置成功</returns>
        public async Task<Boolean> SetStringNoExistsAsync<T>(string key, T value, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return false;
            return await this.ExecuteAsync(CommandType.SETNX, dbNum, async result => await Task.FromResult(result.OK && result.Value.ToInt() > 0), key, this.GetValue(value));
        }
        /// <summary>
        /// 批量设置值 key不存在时
        /// </summary>
        /// <param name="values">key值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public Boolean SetStringNoExists(Dictionary<string, object> values, int? dbNum = null)
        {
            if (values == null || values.Count == 0) return false;
            var list = new List<object>();
            values.Each(v =>
            {
                list.Add(v.Key);
                list.Add(this.GetValue(v.Value));
            });
            return this.Execute(CommandType.MSETNX, dbNum, result => result.OK && result.Value.ToInt() > 0, list.ToArray());
        }
        /// <summary>
        /// 批量设置值 key不存在时 异步
        /// </summary>
        /// <param name="values">key值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public async Task<Boolean> SetStringNoExistsAsync(Dictionary<string, object> values, int? dbNum = null)
        {
            if (values == null || values.Count == 0) return false;
            var list = new List<object>();
            values.Each(v =>
            {
                list.Add(v.Key);
                list.Add(this.GetValue(v.Value));
            });
            return await this.ExecuteAsync(CommandType.MSETNX, dbNum, async result => await Task.FromResult(result.OK && result.Value.ToInt() > 0), list.ToArray());
        }
        /// <summary>
        /// 设置字符串 覆盖给定 key 所储存的字符串值，覆盖的位置从偏移量 offset 开始
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="value">值</param>
        /// <param name="offset">偏移量</param>
        /// <param name="dbNum">库索引</param>
        /// <returns>是否设置成功</returns>
        public Boolean SetString(string key, string value, int offset, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return false;
            return this.Execute(CommandType.SETRANGE, dbNum, result => result.OK, key, offset, value);
        }
        /// <summary>
        /// 设置字符串 覆盖给定 key 所储存的字符串值，覆盖的位置从偏移量 offset 开始 异步
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="value">值</param>
        /// <param name="offset">偏移量</param>
        /// <param name="dbNum">库索引</param>
        /// <returns>是否设置成功</returns>
        public async Task<Boolean> SetStringAsync(string key, string value, int offset, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return false;
            return await this.ExecuteAsync(CommandType.SETRANGE, dbNum, async result => await Task.FromResult(result.OK), key, offset, value);
        }
        /// <summary>
        /// 给指定的key值附加到原来值的尾部
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="value">值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public Boolean AppendString(string key, string value, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return false;
            return this.Execute(CommandType.APPEND, dbNum, result => result.OK, key, value);
        }
        /// <summary>
        /// 给指定的key值附加到原来值的尾部 异步
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="value">值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public async Task<Boolean> AppendStringAsync(string key, string value, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return false;
            return await this.ExecuteAsync(CommandType.APPEND, dbNum, async result => await Task.FromResult(result.OK), key, value);
        }
        #endregion

        #region 获取字符串
        /// <summary>
        /// 获取字符串
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="key">key</param>
        /// <param name="dbNum">库索引</param>
        /// <returns>key的值</returns>
        public T GetString<T>(string key, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return default(T);
            return this.Execute(CommandType.GET, dbNum, result => this.SetValue<T>(result.Value.ToString()), key);
        }
        /// <summary>
        /// 获取字符串
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="dbNum">库索引</param>
        /// <returns>key的值</returns>
        public string GetString(string key, int? dbNum = null) => this.GetString<string>(key, dbNum);
        /// <summary>
        /// 获取字符串 异步
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="key">key</param>
        /// <param name="dbNum">库索引</param>
        /// <returns>key的值</returns>
        public async Task<T> GetStringAsync<T>(string key, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return default(T);
            return await this.ExecuteAsync(CommandType.GET, dbNum, async result => await Task.FromResult(this.SetValue<T>(result.Value.ToString())), key);
        }
        /// <summary>
        /// 获取字符串 异步
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="dbNum">库索引</param>
        /// <returns>key的值</returns>
        public async Task<string> GetStringAsync(string key, int? dbNum = null) => await this.GetStringAsync<string>(key, dbNum);
        /// <summary>
        /// 获取字符串
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="start">起始位置</param>
        /// <param name="end">终止位置</param>
        /// <param name="dbNum">库索引</param>
        /// <returns>key的值的子字符串</returns>
        public string GetString(string key, int start, int end, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return string.Empty;
            return this.Execute(CommandType.GETRANGE, dbNum, result => result.Value.ToString(), key, start, end);
        }
        /// <summary>
        /// 获取字符串 异步
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="start">起始位置</param>
        /// <param name="end">终止位置</param>
        /// <param name="dbNum">库索引</param>
        /// <returns>key的值的子字符串</returns>
        public async Task<string> GetStringAsync(string key, int start, int end, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return string.Empty;
            return await this.ExecuteAsync(CommandType.GETRANGE, dbNum, result => Task.FromResult(result.Value.ToString()), key, start, end);
        }
        /// <summary>
        /// 获取 key 值的长度
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public int GetStringLength(string key, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return -1;
            return this.Execute(CommandType.STRLEN, dbNum, result => result.Value.ToInt(), key);
        }
        /// <summary>
        /// 获取 key 值的长度 异步
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public async Task<int> GetStringLengthAsync(string key, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return -1;
            return await this.ExecuteAsync(CommandType.STRLEN, dbNum, async result => await Task.FromResult(result.Value.ToInt()), key);
        }
        #endregion

        #region 设置key的新值并返回key旧值
        /// <summary>
        /// 设置key的新值并返回key旧值
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="value">key的新值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns>key的旧值</returns>
        public T GetSetString<T>(string key, T value, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return default(T);
            return this.Execute(CommandType.GETSET, dbNum, result => this.SetValue<T>(result.Value.ToString()), key, this.GetValue(value));
        }
        /// <summary>
        /// 设置key的新值并返回key旧值 异步
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="value">key的新值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns>key的旧值</returns>
        public async Task<T> GetSetStringAsync<T>(string key, T value, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return default(T);
            return await this.ExecuteAsync(CommandType.GETSET, dbNum, async result => await Task.FromResult(this.SetValue<T>(result.Value.ToString())), key, this.GetValue(value));
        }
        #endregion

        #region 获取所有(一个或多个)给定key的值
        /// <summary>
        /// 获取所有(一个或多个)给定key的值
        /// </summary>
        /// <param name="dbNum">库索引</param>
        /// <param name="args">key</param>
        /// <returns>按顺序返回key值</returns>
        public List<string> GetString(int? dbNum, params object[] args)
        {
            if (args.Length == 0) return null;
            return this.Execute(CommandType.MGET, dbNum, result => result.Value.ToList<string>(), args);
        }
        /// <summary>
        /// 获取所有(一个或多个)给定key的值 异步
        /// </summary>
        /// <param name="dbNum">库索引</param>
        /// <param name="args">key</param>
        /// <returns>按顺序返回key值</returns>
        public async Task<List<string>> GetStringAsync(int? dbNum, params object[] args)
        {
            if (args.Length == 0) return null;
            return await this.ExecuteAsync(CommandType.MGET, dbNum, result => Task.FromResult(result.Value.ToList<string>()), args);
        }
        /// <summary>
        /// 获取所有(一个或多个)给定key的值
        /// </summary>
        /// <param name="args">key</param>
        /// <returns>按顺序返回key值</returns>
        public List<string> GetString(params object[] args) => this.GetString(null, args);
        /// <summary>
        /// 获取所有(一个或多个)给定key的值 异步
        /// </summary>
        /// <param name="args">key</param>
        /// <returns>按顺序返回key值</returns>
        public async Task<List<string>> GetStringAsync(params object[] args) => await this.GetStringAsync(null, args);
        #endregion

        #region 设置自增长
        /// <summary>
        /// 将 key 所储存的值加上给定的增量值（increment）
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="increment">增量值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public double StringIncrement(string key, double increment, int? dbNum = null) => StringIncrement(key, (float)increment, dbNum);
        /// <summary>
        /// 将 key 所储存的值加上给定的增量值（increment） 异步
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="increment">增量值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public async Task<double> StringIncrementAsync(string key, double increment, int? dbNum = null) => await StringIncrementAsync(key, (float)increment, dbNum);
        /// <summary>
        /// 将 key 所储存的值加上给定的增量值（increment）
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="increment">增量值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public float StringIncrement(string key, float increment, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return -1;
            return this.Execute(CommandType.INCRBYFLOAT, dbNum, result => result.OK ? result.Value.ToCast<float>() : -1, key, increment);
        }
        /// <summary>
        /// 将 key 所储存的值加上给定的增量值（increment） 异步
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="increment">增量值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public async Task<float> StringIncrementAsync(string key, float increment, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return -1;
            return await this.ExecuteAsync(CommandType.INCRBYFLOAT, dbNum, async result => await Task.FromResult(result.OK ? result.Value.ToCast<float>() : -1), key, increment);
        }
        /// <summary>
        /// 将 key 所储存的值加上给定的增量值（increment）
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="increment">增量值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public long StringIncrement(string key, long increment, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return -1;
            return this.Execute(CommandType.INCRBY, dbNum, result => result.OK ? result.Value.ToCast<long>() : -1, key, increment);
        }
        /// <summary>
        /// 将 key 所储存的值加上给定的增量值（increment） 异步
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="increment">增量值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public async Task<long> StringIncrementAsync(string key, long increment, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return -1;
            return await this.ExecuteAsync(CommandType.INCRBY, dbNum, async result => await Task.FromResult(result.OK ? result.Value.ToCast<long>() : -1), key, increment);
        }
        /// <summary>
        /// 将 key 中储存的数字值增一
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public int StringIncrement(string key, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return -1;
            return this.Execute(CommandType.INCR, dbNum, result => result.OK ? result.Value.ToInt() : -1, key);
        }
        /// <summary>
        /// 将 key 中储存的数字值增一 异步
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public async Task<int> StringIncrementAsync(string key, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return -1;
            return await this.ExecuteAsync(CommandType.INCR, dbNum, async result => await Task.FromResult(result.OK ? result.Value.ToInt() : -1), key);
        }
        /// <summary>
        /// key 所储存的值减去给定的减量值（decrement）
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="decrement">减量值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public long StringDecrement(string key, long decrement, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return -1;
            return this.Execute(CommandType.DECRBY, dbNum, result => result.OK ? (long)result.Value : -1, key, decrement);
        }
        /// <summary>
        /// key 所储存的值减去给定的减量值（decrement） 异步
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="decrement">减量值</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public async Task<long> StringDecrementAsync(string key, long decrement, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return -1;
            return await this.ExecuteAsync(CommandType.DECRBY, dbNum, async result => await Task.FromResult(result.OK ? (long)result.Value : -1), key, decrement);
        }
        /// <summary>
        /// 将 key 中储存的数字值减一
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public int StringDecrement(string key, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return -1;
            return this.Execute(CommandType.DECR, dbNum, result => result.OK ? result.Value.ToInt() : -1, key);
        }
        /// <summary>
        /// 将 key 中储存的数字值减一 异步
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public async Task<int> StringDecrementAsync(string key, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return -1;
            return await this.ExecuteAsync(CommandType.DECR, dbNum, async result => await Task.FromResult(result.OK ? result.Value.ToInt() : -1), key);
        }
        #endregion

        #endregion

        #region 排序
        /// <summary>
        /// 排序
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="options">排序选项</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public List<string> Sort(string key, SortOptions options, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return null;
            return this.Execute(CommandType.SORT, dbNum, result => options.Store.IsNullOrEmpty() ? result.Value.ToList<string>() : new List<string> { result.Value.ToString() }, new object[] { key }.Concat(options.ToArgments()).ToArray());
        }
        /// <summary>
        /// 排序 异步 List,Set,SortedSet
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="options">排序选项</param>
        /// <param name="dbNum">库索引</param>
        /// <returns></returns>
        public async Task<List<string>> SortAsync(string key, SortOptions options, int? dbNum = null)
        {
            if (key.IsNullOrEmpty()) return null;
            return await this.ExecuteAsync(CommandType.SORT, dbNum, async result => await Task.FromResult(result.Value.ToList<string>()), new object[] { key }.Concat(options.ToArgments()).ToArray());
        }
        #endregion
    }
}