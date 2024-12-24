FROM python:3.9

RUN pip install kafka-python
RUN pip install paho-mqtt==1.6.1

WORKDIR / 
ADD kafka_producer.py /

CMD /usr/local/bin/python /kafka_producer.py
