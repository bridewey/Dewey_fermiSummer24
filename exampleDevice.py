import asyncio
import os
import sys
import time
import numpy as np

from RedisAdapter import RedisAdapter

class Device:
    def __init__(self, base_key, redis_host):
        self.redis_host = redis_host
        self.base_key = base_key
        self.redis_adapter = None

    async def initialize(self):
        """Initialize the device by establishing a Redis connection."""
        await self.connect_to_redis()

    async def connect_to_redis(self):
        """Connect to Redis."""
        self.redis_adapter = RedisAdapter(self.redis_host)
        await self.redis_adapter.connect()

    async def process_messages(self, messages):
        """Process the messages."""
        count = 0
        for message in messages:
            count += 1
            print(f"Message: {message}")
        return count

    async def runDeviceDataflow(self, data_sub_key):
        """Run the dataflow."""
        """Subscribe to a stream and listen indefinitely."""
        """Start listening for new messages, and process them as they arrive."""
        last_id = '$'  # Start from the last message
        stream_key = f"{self.base_key}:{data_sub_key}"

        print(f"Listening for messages... (stream_key: {stream_key})")
        while True:
            messages, messages_recieved_time = await self.redis_adapter.streamRead({stream_key: last_id}, block=0)
            if messages:
                timerNow = time.time() 
                count = await self.process_messages(messages)
                last_id = messages[-1][1]  # Update last_id
                print (f"Hanlded {count} messages with last ID: {last_id} in {(time.time() - timerNow) * 1000:.3f} milliseconds.")