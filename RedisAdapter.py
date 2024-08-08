import time
import numpy as np
import redis

class RedisAdapter:
    def __init__(self, host, port=6379):
        self.host = host
        self.port = port
        self.connection = None

    async def connect(self):
        """Connect to Redis"""
        try:
            #socket_path = '/home/rsantucc/socket/redis.sock'
            #self.connection = redis.StrictRedis(unix_socket_path=socket_path)
            self.connection = redis.Redis(host=self.host, port=self.port, decode_responses=False)
            if self.connection.ping():
                print(f"Successfully connected to Redis at socket {self.host} synchronously.")
        except redis.ConnectionError:
            print(f"Failed to connect to Redis at {self.host} synchronously.")

    # Example wrapper method for setting a key
    async def set(self, key, value):
        self.connection.set(key, value)

    # Example wrapper method for getting a key
    async def get(self, key):
        return self.connection.get(key)
        
    async def xread(self, streams, count=None, block=None):
        """
        Read from one or more streams.
        :param streams: A dictionary of stream names to stream IDs.
        :param block: Time to block/wait for data (milliseconds).
        :return: The stream entries.
        """
        results = self.connection.xread(streams, count, block)
        return results
    
    async def xadd(self, stream, fields, message_id='*', maxlen=None, approximate=True):
        """
        Append a new entry to a stream.
        
        :param stream: The name of the stream.
        :param fields: A dictionary of field-value pairs to add to the stream entry.
        :param message_id: The ID of the message, typically '*' to let Redis generate it.
        :param maxlen: The maximum length of the stream. If specified, `XADD` trims the stream to this size.
        :param approximate: Whether or not to use approximate trimming. Only relevant if maxlen is specified.
        """
        # Synchronous xadd
        result = self.connection.xadd(stream, fields, message_id, maxlen, approximate)
        #print(f"Added message to stream {stream} with ID {result}")

    async def xrevrange(self, stream, start='+', end='-', count=1):
        """
        Retrieve elements from a stream using XREVRANGE with specified range and limit.

        :param stream: The name of the stream.
        :param start: The start ID, defaults to '+' (newest entry).
        :param end: The end ID, defaults to '-' (oldest entry).
        :param count: The number of elements to retrieve, defaults to 1.
        :return: The elements from the specified range of the stream.
        """
        range_elements = self.connection.xrevrange(stream, start, end, count)
        return range_elements

    async def streamRead(self, streams, block=None):
        """
        Read from streams and parse message data from binary to float.

        Assumes that the stream field values are encoded as binary representations
        of 32-bit floats.

        :param streams: A dictionary of stream names to stream IDs.
        :param block: Time to block/wait for data (milliseconds).
        :return: A list of stream entries with data parsed as floats.
        """
        parsed_messages = []
        start_time = time.time()
        messages = await self.xread(streams, block=block)
        messages_received_time = time.time()
        print(f"Read {len(messages)} messages in {(messages_received_time - start_time) * 1000:.3f} milliseconds.")

        for stream, message_data in messages:
            for message_id, fields in message_data:
                parsed_fields = {}
                for key, value in fields.items():
                    if key == b'_':
                        parsed_fields['_'] = np.frombuffer(value, dtype=np.float32)
                    else:
                        parsed_fields[key] = value
                parsed_messages.append((stream, message_id, parsed_fields))

        return parsed_messages, messages_received_time
    
    async def streamReadRange(self, stream, start='+', end='-', count=1, dtype=np.float32):
        """
        Retrieve and parse elements from a stream using XREVRANGE with specified range and limit.

        :param stream: The name of the stream.
        :param start: The start ID, defaults to '+' (newest entry).
        :param end: The end ID, defaults to '-' (oldest entry).
        :param count: The number of elements to retrieve, defaults to 1.
        :return: Parsed elements from the specified range of the stream.
        """
        range_elements =await self.xrevrange(stream, start=start, end=end, count=count)
        parsed_elements = []

        for element in range_elements:
            message_id, fields = element
            parsed_fields = {}
            for key, value in fields.items():
                    if key == b'_':
                        parsed_fields['_'] = np.frombuffer(value, dtype=dtype)
                    else:
                        parsed_fields[key] = value
            parsed_elements.append((stream, message_id, parsed_fields))

        return parsed_elements
    
    async def streamAdd(self, stream, fields, message_id='*', maxlen=None, approximate=True):
        """
        Append a new entry to a stream, converting data to binary before adding.

        :param stream: The name of the stream.
        :param fields: A dictionary of field-value pairs to add to the stream entry.
        :param message_id: The ID of the message, '*' to let Redis generate it.
        :param maxlen: The maximum length of the stream. If specified, `XADD` trims the stream to this size.
        :param approximate: Whether or not to use approximate trimming. Only relevant if maxlen is specified.
        """
        for key, value in fields.items():
            if isinstance(value, np.ndarray):
                fields[key] = fields[key] = value.astype(np.int16).tobytes()
        await self.xadd(stream, fields, message_id=message_id, maxlen=maxlen, approximate=approximate)

    async def streamAddPipeline(self, device_key, streams, message_id='*', maxlen=None, approximate=True):
        """
        Append a new entry to a stream using a pipeline, converting data to binary before adding.

        :param stream: The name of the stream.
        :param fields: A dictionary of field-value pairs to add to the stream entry.
        :param message_id: The ID of the message, typically '*' to let Redis generate it.
        :param maxlen: The maximum length of the stream. If specified, `XADD` trims the stream to this size.
        :param approximate: Whether or not to use approximate trimming. Only relevant if maxlen is specified.
        """
        start_time = time.time()
        pipeline = self.connection.pipeline()
        for stream, data in streams.items():
            binary_data = {'_': data.tobytes()}
            stream_key = f"{device_key}:{stream}"
            pipeline.xadd(stream_key, binary_data, id=message_id, maxlen=maxlen, approximate=approximate)
        pipeline.execute()
        print(f"Pipeline for device_key finished in {(time.time() - start_time) * 1000000:.3f} microseconds.")
    
