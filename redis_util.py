#coding=utf-8
from redis import StrictRedis


class RedisUtils(object):
    def __init__(self, host, port, db, password=None):
        if password is not None:
            self._client = StrictRedis(host=host, port=port, db=db, password=password)
        else:
            self._client = StrictRedis(host=host, port=port, db=db)

    def common_utils(self):
        """
        通用操作
        :return:
        """
        pass

    def collection_utils(self):
        """
        统计操作
        :return:
        """
        pass

    def string_utils(self):
        """
        字符串操作
        :return:
        """
        str_name = "str"
        str_value = "str_value"
        int_name = "number"
        int_value = 25
        int_str_name = "num_to_str"
        int_str_value = 10000
        int_max_name = "max_number"
        int_max_value = 2444449999999999999999999
        int_min_name = "min_number"
        int_min_value = -1111111111111111111111111
        float_name = "float_number"
        float_value = 23.45
        result = self._client.set(str_name, str_value)
        result_len = self._client.strlen(str_name)
        print("String Set key:", str_name, " value:", str_value, " result:", result, " len:", result_len)
        result = self._client.set(int_name, int_value)
        result_len = self._client.strlen(int_name)
        print("String Set key:", int_name, " value:", int_value, " result:", result, " len:", result_len)
        result = self._client.set(int_str_name, int_str_value)
        result_len = self._client.strlen(int_str_name)
        print("String Set key:", int_str_name, " value:", int_str_value, " result:", result, " len:", result_len)
        result = self._client.set(int_max_name, int_max_value)
        result_len = self._client.strlen(int_max_name)
        print("String Set key:", int_max_name, " value:", int_max_value, " result:", result, " len:", result_len)
        result = self._client.set(int_min_name, int_min_value)
        result_len = self._client.strlen(int_min_name)
        print("String Set key:", int_min_name, " value:", int_min_value, " result:", result, " len:", result_len)
        result = self._client.set(float_name, float_value)
        result_len = self._client.strlen(float_name)
        print("String Set key:", float_name, " value:", float_value, " result:", result, " len:", result_len)

        str_dict = {"str_key": "keyvalue", "int_key": 234, "float_key": 234.09}
        result = self._client.mset(str_dict)
        print("String MSet dict:", str_dict, " result:", result)

        null_str_name = ""
        null_str_value = self._client.get(null_str_name)
        print("String Get key:", null_str_name, " value:", null_str_value)
        dest_str_value = self._client.get(str_name)
        print("String Get key:", str_name, " value", dest_str_value, " type:", type(dest_str_value))
        dest_int_value = self._client.get(int_name)
        print("String Get key:", int_name, " value", dest_int_value, " type:", type(dest_int_value))
        multi_get_name = ["str_key","int_key", "float_key"]
        result = self._client.mget(multi_get_name)
        result_dict = dict(zip(multi_get_name, result))
        print("String MGet key:", multi_get_name, " values:", result, " type:", type(result)
              , " dict:", result_dict)

        ### 自增 自减 支持负数增减
        incr_name = "incr_name"
        incr_count = 2
        result = self._client.incr(incr_name, incr_count)
        print("String Incr key:", incr_name, "incr_count:", incr_count, " result:", result)
        incr_count = -2
        result = self._client.incr(incr_name, incr_count)
        print("String Incr key:", incr_name, "incr_count:", incr_count, " result:", result)
        decr_name = "decr_name"
        decr_count = -2
        result = self._client.decr(decr_name, decr_count)
        print("String Decr key:", decr_name, "incr_count:", decr_count, " result:", result)
        decr_count = 2
        result = self._client.decr(decr_name, decr_count)
        print("String Decr key:", decr_name, "incr_count:", decr_count, " result:", result)

    def list_utils(self):
        not_existed_list_name = "not exist list name"
        list_name = "list_key"
        list_str_value = "list value"
        list_int_value = 234
        # O(1)
        result = self._client.rpush(list_name, list_str_value, list_int_value)
        print("List RPUSH key:", list_name, "value:", list_int_value, list_str_value, "result:", result)
        list_value = ("list_value_1", "list_value_2", 234)
        # O(1)
        result = self._client.rpush(list_name, *list_value)
        print("List RPUSH key:", list_name, "value:", list_value, "result:", result)
        # O(X)
        result = self._client.rpushx(list_name, list_value)
        print("List RPUSHX key:", list_name, "value:", list_value, "result:", result)
        result = self._client.rpushx(not_existed_list_name, list_value)
        print("List RPUSHX key:", not_existed_list_name, "value:", list_value, "result:", result)

        # O(1)
        result = self._client.lpop(list_name)
        print("List LPOP key:", list_name, " value:", result)
        # O(1)
        result = self._client.rpop(list_name)
        print("List RPOP key:", list_name, " value:", result)

        # O(n)
        result = self._client.lrem(list_name, 1, 231)
        print("List LREM key:", list_name, "result:", result)
        # O(n)
        lstart = 0
        lend = 2
        result = self._client.ltrim(list_name, lstart, lend)
        print("List LTRIM key:", list_name, " result:", result)

        # O(1)
        result = self._client.llen(list_name)
        print("List LLEN key:", list_name, " len:", result)

        # O(X)
        result = self._client.linsert(list_name, "before", 234, "Insert Value")
        print("List LINSERT key:", list_name, " result:", result)

        lindex = 2  # Start As 0
        # O(X)
        result = self._client.lindex(list_name, lindex)
        print("List LINDEX key:", list_name, " result:", result)

    def set_utils(self):
        """
        集合操作
        分类 社交 标签
        :return:
        """
        set_name = "set_name"
        set_value = ("set_1", "set_2", 3, 4)
        # O(K)
        result = self._client.sadd(set_name, *set_value)
        print("Set SADD key:", set_name, "value:", set_value, " result:", result)
        # O(1)
        result =self._client.scard(set_name)
        print("Set SCARD key:", set_name, " result:", result)
        s_find_value = "set_1"
        # O(1)
        result = self._client.sismember(set_name, s_find_value)
        print("Set SISMEMBER key:", set_name, " find_value:", s_find_value, " result:", result)

        random_count = 2
        # O(K)
        result = self._client.srandmember(set_name, number=random_count)
        print("Set SRANDOMMEMBER key:", set_name, "result:", result)

        # O(1)
        result = self._client.spop(set_name)
        print("Set SPOP key:", set_name, " result:", result)

        # O(K)
        result = self._client.srem(set_name, *set_value)
        print("Set SREM key:", set_name, "value:", set_value, " result:", result)

        set_a_name = "set_a"
        set_a_value = ["set_value_1", "set_value_3", "set_value_6", "set_value_9", "set_value_10"]
        set_b_name = "set_b"
        set_b_value = ["set_value_1", "set_value_3", "set_value_6", "set_value_8", "set_value_0"]
        self._client.sadd(set_a_name, *set_a_value)
        self._client.sadd(set_b_name, *set_b_value)
        # O(K)
        result = self._client.sinter(set_a_name, set_b_name)
        print("Set SINTER key:", set_a_name, " key:", set_b_name, " result:", result)
        # O(K)
        result = self._client.sunion(set_a_name, set_b_name)
        print("Set SUNION key:", set_a_name, " key:", set_b_name, " result:", result)
        # O(K)
        result = self._client.sdiff(set_a_name, set_b_name)
        print("Set SDIFF key:", set_a_name, " key:", set_b_name, " result:", result)
        self._client.delete(set_a_name)
        self._client.delete(set_b_name)

    def zset_utils(self):
        """
        集合操作
        应用排行榜
        :return:
        """
        zset_name = "zset_name"
        zset_value = {"zset_1": 23, "zset_2": 23.56, "zset_3": 23, "zset_4": -4, "zset_5": 0}
        result =self._client.zadd(zset_name, **zset_value)
        print("zset ZADD key:", zset_name, " result:", result)
        result = self._client.zcard(zset_name)
        print("zset ZCARD key:", zset_name, " result:", result)
        zset_score_value = "zset_4"
        result = self._client.zscore(zset_name, zset_score_value)
        print("zset ZSCORE key:", zset_name, "value:", zset_score_value," result:", result)
        result = self._client.zrank(zset_name, zset_score_value)
        print("zset ZRANK key:", zset_name, "value:", zset_score_value, " result:", result)
        result = self._client.zrevrank(zset_name, zset_score_value)
        print("zset ZREVRANK key:", zset_name, "value:", zset_score_value, " result:", result)
        zset_cursor = 0
        index, result = self._client.zscan(zset_name, zset_cursor)
        print("zset ZSCAN key:", zset_name, " result:", result, "type:", type(result))

        zset_min = 0  # Start From 0
        zset_max = 2
        zset_num = 2
        result = self._client.zrange(zset_name, zset_min, zset_max)
        print("zset ZRANGE key:", zset_name, "min:", zset_min, " max:", zset_max, " result:", result)
        # self._client.zrangebylex(zset_name, 0, 2, num=2)
        # self._client.zrangebyscore(zset_name, 0, 2, num=2)
        self._client.delete(zset_name)

    def hash_utils(self):
        hash_name = "hash_name"
        hash_key = "hask_key"
        hash_value = "hash_value"
        result = self._client.hset(hash_name, hash_key, hash_value)
        print("hash HSET key:", hash_name, " field:", hash_key, " value:", hash_value, " result:", result)
        hash_content = {"name": "lee", "age": 34, "birth": 2009}
        result = self._client.hmset(hash_name, hash_content)
        print("hash HMSET content:", hash_content, " result:", result)
        result = self._client.hlen(hash_name)
        print("hash HLEN key:", hash_name, " result:", result)
        result = self._client.hexists(hash_name, hash_key)
        print("hash HEXISTS key:", hash_name, " field:", hash_key, " result:", result)
        result = self._client.hget(hash_name, hash_key)
        print("hash HGET key:", hash_name, " field:", hash_key, " result:", result)
        hash_keys = ("name", "age")
        result = self._client.hmget(hash_name, *hash_keys)
        print("hash HMGET key:", hash_name, " field:", hash_keys, " result:", result)
        hash_cursor = 0
        result = self._client.hscan(hash_name, hash_cursor)
        print("hash HSCAN key:", hash_name, " result:", result)
        result = self._client.hkeys(hash_name)
        print("hash HKEYS key:", hash_name, "result:", result)
        result =self._client.hdel(hash_name, hash_key)
        print("hash HDEL key:", hash_name, " field:", hash_key, " result:", result)
        result =self._client.hdel(hash_name, *hash_keys)
        print("hash HDEL key:", hash_name, " field:", hash_key, " result:", result)


if __name__ == "__main__":
    redis_ip = "10.160.34.113"
    redis_port = 10002
    redis_db = 1
    redis_server = RedisUtils(redis_ip, redis_port, redis_db)

    redis_server.hash_utils()
    # redis_server.zset_utils()
    # redis_server.set_utils()
    # redis_server.list_utils()
    # redis_server.string_utils()



