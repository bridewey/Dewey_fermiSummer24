# fermiSummer24
Brianna Dewey
SIST Project - Summer 2024
Integration Development and Testing of Rear Transition Monitor for Beam Current Monitoring System


"Resources" Google Doc: https://docs.google.com/document/d/1IK7vZAGgKBvf8NjGWcgfm5q_Jsp-FncC1CbgZqidTkw/edit

Currently have a container on bidaqt and some of the automation steps (sorta) working via run.bash
- Python script called bcmProducer.py will need to be edited to allow for use of data acquired with run.bash

rpsa_client-2.00-35-aff683518 (directory) has many things inside: 
- came with: default_dac_config.json, default_multiple_dac_config.json, rpsa_client, and convert_tool
- added: bcmProducer.ipynb, bcmProducer.py, exampleDevice.py, pydapter.py, RedisAdapter.py, run.bash, data and data2 (subfolders)

exampleDevice.py, pydapter.py, RedisAdapter.py code came from Bobby

Can use ./rpsa_client --h to see configuration settings for various streaming modes

Most recent / "final versions" (for Brianna's purposes) of all files necessary for this project are located in Dewey_LastDay_Edits.zip
** things will definitely need some changing/editing
** I decided to leave other files in here so you could see old iterations / because I don't really know how github works (sorry)
- dataRun.sh: collects data with beam+baseline configuration (need to make sure that generators are set up to "stream A" configuration BEFORE running data using ./dataRun.sh in terminal); puts data stream with STREAM_NAME = "DATA" into subfolder and runs bcmProducer.py
- baselineRun.sh: collects data with baseline configuration (need to make sure that generators are set up to "stream B" configuration BEFORE running data using ./baselineRun.sh in terminal); puts data stream with STREAM_NAME = "BASELINE" into subfolder and runs bcmProducer.py
- bcmProducer.py: takes in data (csv file) and streams to Redis with the appropriate stream name
- bcmConsumer.py: takes in data files by stream keys from Redis; has different functions defined to perform various tasks, such as "main" which reads data from Redis stream, "analyze" (doing subtraction step), "process_signal" which does the other processing steps, and "streamBack" which streams processed data back to Redis

If you have any questions, feel free to reach out to me at either bdewey@nd.edu or briannadewey26@gmail.com
