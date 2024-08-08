# fermiSummer24
Brianna Dewey
SIST Project - Summer 2024

"Resources" Google Doc: https://docs.google.com/document/d/1IK7vZAGgKBvf8NjGWcgfm5q_Jsp-FncC1CbgZqidTkw/edit

Currently have a container on bidaqt and some of the automation steps working via run.bash

rpsa_client-2.00-35-aff683518 (directory) has many things inside: 
- came with: default_dac_config.json, default_multiple_dac_config.json, rpsa_client, and convert_tool
- added: bcmProducer.ipynb, bcmProducer.py, exampleDevice.py, pydapter.py, RedisAdapter.py, run.bash, data and data2 (subfolders)

Can use ./rpsa_client --h to see configuration settings for various streaming modes

Python script called bcmProducer.py will need to be edited to allow for use of data acquired with run.bash

