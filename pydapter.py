import redis
import json

class RedisAdapterSingle:
    def __init__(self, base_key, connection_string='redis://127.0.0.1:6379/0'):
        self.redis = redis.Redis.from_url(connection_string)
        self.base_key = base_key
        self.config_key = f"{base_key}:CONFIG"
        self.log_key = f"{base_key}:LOG"
        self.channel_key = f"{base_key}:CHANNEL"
        self.status_key = f"{base_key}:STATUS"
        self.time_key = f"{base_key}:TIME"
        self.data_key = f"{base_key}:DATA"
        self.device_key = f"{base_key}:DEVICES"
        self.abort_key = f"{base_key}:ABORT"
        # Setup other keys based on base_key as needed

    def get_devices(self):
        return list(self.redis.smembers(self.device_key))

    def clear_devices(self, devicelist):
        for device in devicelist:
            self.redis.delete(f"{self.device_key}:{device}")

    def set_device_config(self, config):
        self.redis.hmset(self.config_key, config)

    def get_device_config(self):
        return self.redis.hgetall(self.config_key)

    def set_device(self, name):
        self.redis.sadd(self.device_key, name)

    def get_value(self, key):
        return self.redis.get(key)

    def set_value(self, key, value):
        self.redis.set(key, value)

    def get_unique_value(self, key):
        return self.redis.incr(key)

    def get_hash(self, key):
        return self.redis.hgetall(key)

    def set_hash(self, key, hash_map):
        self.redis.hmset(key, hash_map)

    def get_set(self, key):
        return self.redis.smembers(key)

    def set_set(self, key, value):
        self.redis.sadd(key, value)

    def stream_write(self, data, time_id, key, trim=0):
        stream_id = self.redis.xadd(key, fields=data, id=time_id)
        if trim > 0:
            self.redis.xtrim(key, max_len=trim, approximate=True)

    def stream_read(self, key, start, end, count):
        return self.redis.xrange(key, min=start, max=end, count=count)

    def stream_trim(self, key, size):
        self.redis.xtrim(key, max_len=size, approximate=False)

    def publish(self, message, channel=None):
        full_channel = f"{self.channel_key}:{channel}" if channel else self.channel_key
        self.redis.publish(full_channel, message)

    # Additional methods to implement: subscribe, psubscribe, streamReadBlock, logWrite, logRead, etc.

