# tp05 — real-time data streaming and processing using apache kafka

## 1. what problem does apache kafka solve in a data-streaming architecture?

apache kafka allows applications to exchange and process large volumes of real-time events reliably. it decouples producers from consumers and allows multiple applications to process the same data independently.

## 2. define producer, consumer, broker, topic, partition, offset, and consumer group.

- **producer:** an application that sends messages to kafka.
- **consumer:** an application that reads messages from kafka.
- **broker:** a kafka server that stores and manages messages.
- **topic:** a named stream or category of messages.
- **partition:** a division of a topic that stores an ordered sequence of messages.
- **offset:** the position of a message within a partition.
- **consumer group:** a group of consumers that share the work of reading topic partitions.

## 3. what is the difference between a kafka topic and a partition?

a topic is a logical category of messages, while a partition is a division of that topic. partitions allow kafka to distribute data and support parallel consumption.

## 4. why does the consumer use `auto_offset_reset='earliest'`? when does this setting apply?

it tells the consumer to start reading from the earliest available message when the consumer group has no previously committed offset. it does not override an existing committed offset.

## 5. what is the purpose of the consumer group id? what happens if a new group id is used?

the group id identifies a consumer group and allows kafka to track its reading progress. a new group has independent offsets and can read the topic's messages independently. with `auto_offset_reset='earliest'`, it starts from the earliest available messages if no offsets have been committed.

## 6. why is json a suitable format for this exercise? give one limitation.

json is lightweight, human-readable, and easy to generate and parse in python. it supports structured sensor data. one limitation is that it can be less compact and efficient than binary formats.

## 7. explain how docker compose simplifies deployment compared with starting services manually.

docker compose defines the kafka broker, configuration, port mappings, and persistent storage in one yaml file. it simplifies managing the environment with a few commands.

## 8. what are the limitations of this single-broker educational setup compared with a production kafka cluster?

the setup has no broker redundancy or high availability. a broker failure makes the service unavailable. replication factor 1 provides no fault tolerance, and scalability is limited compared with a multi-broker cluster.

## 9. attach evidence showing the running container, topic description, producer output, and consumer output.

the required screenshots are included in the `screenshots/` directory:

- docker compose status.
- kafka topic description.
- producer output.
- consumer output.

## problems encountered and solutions

during installation, `kafka-python==2.0.2` produced a `ModuleNotFoundError` involving `kafka.vendor.six.moves`. installing the external `six` package and modifying the import in `kafka/codec.py` resolved the issue while preserving the required kafka client version.

python 3.13 was installed using mise without replacing the system python.
