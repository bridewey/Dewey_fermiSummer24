# bcmProducer.ipynb but as a .py file instead

import asyncio
import random
import time
import numpy as np
import os
import sys
import pandas as pd
from exampleDevice import Device

# ----------------
# producer section
# ----------------
async def main():
    device = Device('LINAC:BCM1', 'brianna_redis')
    await device.initialize()

    # "stream A" - just noise
    data_file_path1 = 'data/tek0002CH2_added_streamA.csv'
    numpy_data1 = np.genfromtxt(data_file_path1, dtype=np.int16)
    
    # "stream B" - signal and noise
    data_file_path2 = 'data/tek0002CH2_added_streamB.csv'
    numpy_data2 = np.genfromtxt(data_file_path2, dtype=np.int16)
    
    stream_key_raw = f"{device.base_key}:RAW_DATA" 
    stream_key_noise = f"{device.base_key}:RAW_DATA_NOISE"
    binary_field = '_'
    data_type_field = 'TYPE'
    data_type = 'INT16'
    
    # raw data, aka "stream A"
    data_point_raw = {binary_field: numpy_data1, data_type_field: data_type}
    await device.redis_adapter.streamAdd(stream_key_raw, data_point_raw, maxlen=10)
    messagesA = await device.redis_adapter.streamReadRange( stream_key_raw, start='+', end='-', count=3, dtype=np.int16)
    messageA = messagesA[0]
    streamA, timeA, dataA = messageA
    
    # noise data, aka "stream B"
    data_point_noise = {binary_field: numpy_data2, data_type_field: data_type}
    await device.redis_adapter.streamAdd(stream_key_noise, data_point_noise, maxlen=10)
    messagesB = await device.redis_adapter.streamReadRange( stream_key_noise, start='+', end='-', count=1, dtype=np.int16)
    messagesB = await device.redis_adapter.streamReadRange( stream_key_noise, start='+', end='-', count=3, dtype=np.int16)
    messageB = messagesB[0]
    streamB, timeB, dataB = messageB
    
    
    # ----------------
    # consumer section: 
    # ----------------
    df_streamA = pd.DataFrame(dataA['_'])
    #print(df_streamA)
    df_streamB = pd.DataFrame(dataB['_'])
    #print(df_streamB)
    
    # step 1 - subtraction
    df_subtraction = df_streamA - df_streamB
    df_subtraction = df_subtraction.iloc[2400:3401]
    
    #def process_signal(df_streamA, df_streamB, sampling_rate=100000, f_corner=70, window_size=3):
    def process_signal(df_subtraction, sampling_rate=100000, f_corner=70, window_size=3):
        
        # step 2 - droop correction
        time_step = 1 / sampling_rate 
        droopCorr = 2 * np.pi * f_corner
        num_samples = len(df_subtraction)
        integrated = 0.0
        droopCorr_signal = pd.Series(index=df_subtraction.index, dtype=float)
    
        for i in range(num_samples - 1): # loop up to num_samples - 1 to avoid accessing out of bounds
            integrated += df_subtraction.iloc[i].iloc[0] * time_step
            droopCorr_signal[i] = df_subtraction.iloc[i].iloc[0]  + integrated * droopCorr
    
        # if not along origin on y axis, shift signal downward
        min_value = droopCorr_signal.min()
        droopCorr_shifted_signal = droopCorr_signal - min_value
    
        # step 4 - mode filter
        mode_filtered = droopCorr_signal.rolling(window=window_size, min_periods=1).apply(lambda x: x.mode()[0])
    
        # step 5 - integrate
        integral_mode = mode_filtered.sum()
    
        # will need to stream this value back to redis
        return integral_mode
        return df_streamA, df_streamB, df_subtraction, droopCorr_signal, droopCorr_shifted_signal, mode_filtered, integral_mode
    

    # ----------------
    # "run" the function
    # ----------------
        integral_mode, df_streamA, df_streamB, df_subtraction, droopCorr_signal, droopCorr_shifted_signal, mode_filtered = process_signal(df_subtraction)

    #integral_mode = process_signal(df_subtraction)
    #print("Integral of mode_filtered:", integral_mode)
    
    
    # ----------------
    # return value to redis
    # ----------------
    stream_key_processed = f"{device.base_key}:PROCESSED_DATA"
    data_point_processed = {
        binary_field: np.array([integral_mode], dtype=np.int16),
        data_type_field: data_type  # Assuming data_type is defined as 'INT16'
    }
    
    await device.redis_adapter.streamAdd(stream_key_processed, data_point_processed, maxlen=10)
    messagesC = await device.redis_adapter.streamReadRange( stream_key_processed, start='+', end='-', count=1, dtype=np.int16)
    messagesC = await device.redis_adapter.streamReadRange( stream_key_processed, start='+', end='-', count=3, dtype=np.int16)
    messageC = messagesC[0]
    streamC, timeC, dataC = messageC

if __name__ == "__main__":
        asyncio.run(main())


