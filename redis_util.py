#coding=utf-8
from redis import StrictRedis


class RedisUtils(object):
    def __init__(self, host, port, db, password=None):
        if password is not None:
            self._client = StrictRedis(host=host, port=port, db=db, password=password)
        else:
            self._client = StrictRedis(host=host, port=port, db=db)

    def string_utils(self):
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

        ### 自增 自减
        incr_name = "incr_name"
        incr_count = 2
        result = self._client.incr(incr_name, incr_count)
        print("String Incr key:", incr_name, "incr_count:", incr_count, " result:", result)
        incr_count = -2
        result = self._client.incr(incr_name, incr_count)
        print("String Incr key:", incr_name, "incr_count:", incr_count, " result:", result)


if __name__ == "__main__":
    redis_ip = "10.160.34.113"
    redis_port = 10002
    redis_db = 1
    redis_server = RedisUtils(redis_ip, redis_port, redis_db)
    redis_server.string_utils()



