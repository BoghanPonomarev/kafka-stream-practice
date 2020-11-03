#### Part 3 - excactly one producer and consumers with json date and grouping() & aggregate()

kafka-topics --bootstrap-server localhost:9092 --topic bank-transaction --create --partitions 3 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 --topic bank-balance --create --partitions 3 --replication-factor 1

kafka-console-consumer --bootstrap-server localhost:9092 --topic bank-balance

**All input date in application, just start Demo.class**