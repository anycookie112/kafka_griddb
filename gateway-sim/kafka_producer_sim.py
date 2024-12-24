# #!/usr/bin/python3

# from kafka import KafkaProducer
# import json
# import datetime
# import time as t
# import logging
# import random
# import threading

# NUM_DEVICES=10


# logging.basicConfig(format='%(asctime)s %(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S',
#                     filename='producer.log',
#                     filemode='w')

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

# p=KafkaProducer(bootstrap_servers=['broker:9092'])
# print('Kafka Producer has been initiated...')

# def register_device(meterid):
#      data= {
#         "payload": 
#         {
#             'meterid': meterid,
#             'billdate': random.randint(1, 28) 
#         },
#         "schema": 
#         {
#             "fields": [ 
#                 { "field": "meterid", "optional": False, "type": "int32" },
#                 { "field": "billdate", "optional": False, "type": "int32" } 
#             ], 
#             "name": "device", "optional": False, "type": "struct" 
#         }    
#      }
#      m=json.dumps(data, indent=4, sort_keys=True, default=str)
#      print("Registering",m)
#      p.send("meters", m.encode('utf-8'))
 
    
# def produce(meterid, usagemodel=None):
#     time = datetime.datetime.now()-datetime.timedelta(days=100)
#     register_device(meterid)

#     base_temp = random.uniform(-10,40)
#     base_kwh = random.uniform(0,2)
#     while True:
#         now = time.strftime('%Y-%m-%d %H:%M:%S.%f')
#         data= {
#             "payload": 
#             {
#                 'timestamp': now,
#                 'kwh': base_kwh+random.uniform(-.2, 2),
#                 'temp': base_temp+random.uniform(-5, 5) 
#             },
#             "schema": 
#             {
#                 "fields": [ 
#                     { "field": "timestamp", "optional": False, "type": "string" },
#                     { "field": "kwh", "optional": False, "type": "double" }, 
#                     { "field": "temp", "optional": False, "type": "double" } 
#                 ], 
#                 "name": "iot", "optional": False, "type": "struct" 
#             }    
#          }
#         time = time + datetime.timedelta(minutes=60)
#         if time > datetime.datetime.now():
#             time.sleep(3600)

#         m=json.dumps(data, indent=4, sort_keys=True, default=str)
#         p.send("meter_"+str(meterid), m.encode('utf-8'))
#         print("meter_"+str(meterid), data['payload'])

# def main():
   
#     i=0;
#     threads={}
#     while i < NUM_DEVICES: 
#         threads[i] = threading.Thread(target=produce, args=(i,))
#         threads[i].start()
#         i=i+1
    

# if __name__ == '__main__':
#     main()


# from kafka import KafkaProducer
# import json
# import datetime
# import time as t
# import logging
# import threading
# import paho.mqtt.client as mqtt

# mqtt_broker = "192.168.1.15"
# mqtt_topic = "machine_1"

# # Initialize MQTT Client
# mqtt_client = mqtt.Client("mqtt_bridge", clean_session=True)

# # Initialize Kafka Producer
# p = KafkaProducer(bootstrap_servers=['192.168.1.15:9092'])
# print('Kafka Producer has been initiated...')

# # Setup logging
# logging.basicConfig(format='%(asctime)s %(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S',
#                     filename='producer.log',
#                     filemode='w')

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

# # MQTT on_message callback
# def on_message(client, userdata, message):
#     """Callback for handling incoming MQTT messages."""
#     try:
#         msg_payload = message.payload.decode()
#         print(f"Received MQTT message: {msg_payload}")
#         data = json.loads(msg_payload)
#         state = data.get('state')
#         machineid = data.get('machine_id')
#         print("Received state:", state)
#         print("Received machine_id:", machineid)
        
#         # Produce the data to Kafka after receiving MQTT message
#         produce(machineid, state)

#     except Exception as e:
#         print(f"Error processing message: {e}")

# # Connect to MQTT Broker
# mqtt_client.connect(mqtt_broker)
# print(f"Connected to MQTT broker at {mqtt_broker}")

# # Subscribe to the MQTT topic
# mqtt_client.subscribe(mqtt_topic)
# print(f"Subscribed to MQTT topic '{mqtt_topic}'")

# # Start the MQTT client loop in a separate thread
# mqtt_client.on_message = on_message
# mqtt_client.loop_start()

# # Kafka Production Function
# def produce(machineid, state):
#     """Produce data to Kafka."""
#     time = datetime.datetime.now()

#     while True:
#         now = time.strftime('%Y-%m-%d %H:%M:%S.%f')
#         data = {
#             "payload": {
#                 'timestamp': now,
#                 'state': state
#             },
#             "schema": {
#                 "fields": [
#                     {"field": "timestamp", "optional": False, "type": "string"},
#                     {"field": "state", "optional": False, "type": "string"}
#                 ],
#                 "name": "machine_1", "optional": False, "type": "struct"
#             }
#         }
        
#         # Send the data to Kafka
#         m = json.dumps(data, indent=4, sort_keys=True, default=str)
#         p.send(machineid, m.encode('utf-8'))
#         print(f"Produced message to Kafka: {m}")

#         # Simulate periodic production of messages
#         time = time + datetime.timedelta(minutes=60)
#         if time > datetime.datetime.now():
#             t.sleep(3600)

# # Keep the script running
# try:
#     while True:
#         t.sleep(1)
# except KeyboardInterrupt:
#     print("Exiting...")
#     mqtt_client.loop_stop()

from kafka import KafkaProducer
import json
import datetime
import time as t
import logging
import threading
import paho.mqtt.client as mqtt

mqtt_broker = "192.168.1.15"
mqtt_topic = "machine_1"

# Initialize MQTT Client
mqtt_client = mqtt.Client("mqtt_bridge", clean_session=True)

# Initialize Kafka Producer
p = KafkaProducer(bootstrap_servers=['192.168.1.15:9092'])
print('Kafka Producer has been initiated...')

# Setup logging
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# MQTT on_message callback
def on_message(client, userdata, message):
    """Callback for handling incoming MQTT messages."""
    try:
        msg_payload = message.payload.decode()
        print(f"Received MQTT message: {msg_payload}")
        data = json.loads(msg_payload)
        state = data.get('state')
        machineid = data.get('machine_id')
        print("Received state:", state)
        print("Received machine_id:", machineid)
        
        # Produce the data to Kafka after receiving MQTT message
        produce(machineid, state)

    except Exception as e:
        print(f"Error processing message: {e}")

# Connect to MQTT Broker
mqtt_client.connect(mqtt_broker)
print(f"Connected to MQTT broker at {mqtt_broker}")

# Subscribe to the MQTT topic
mqtt_client.subscribe(mqtt_topic)
print(f"Subscribed to MQTT topic '{mqtt_topic}'")

# Start the MQTT client loop in a separate thread
mqtt_client.on_message = on_message
mqtt_client.loop_start()

# Kafka Production Function
def produce(machineid, state):
    """Produce data to Kafka."""
    time = datetime.datetime.now()
    now = time.strftime('%Y-%m-%d %H:%M:%S.%f')
    
    # Prepare the data to send
    data = {
        "payload": {
            'timestamp': now,
            'state': state
        },
        "schema": {
            "fields": [
                {"field": "timestamp", "optional": False, "type": "string"},
                {"field": "state", "optional": False, "type": "string"}
            ],
            "name": "machine_1", "optional": False, "type": "struct"
        }
    }

    # Send the data to Kafka asynchronously
    m = json.dumps(data, indent=4, sort_keys=True, default=str)
    p.send(machineid, m.encode('utf-8'))
    print(f"Produced message to Kafka: {m}")

# Keep the script running
try:
    while True:
        t.sleep(1)
except KeyboardInterrupt:
    print("Exiting...")
    mqtt_client.loop_stop()
