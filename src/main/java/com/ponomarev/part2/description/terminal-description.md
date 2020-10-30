#### Part 2 variant 1 - with keys

**Topics creation** 

kafka-topics --bootstrap-server localhost:9092 --topic user-colors-input --create --partitions 3 --replication-factor 1 

kafka-topics --bootstrap-server localhost:9092 --topic user-colors-output --create --partitions 3 --replication-factor 1 

**Consumer/Producer** 

kafka-console-consumer.bat --bootstrap-server localhost:9092 ^    --topic user-colors-output ^    --from-beginning ^    --formatter kafka.tools.DefaultMessageFormatter ^    --property print.key=true ^    --property print.value=true ^    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer ^    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

kafka-console-producer --bootstrap-server localhost:9092 --topic user-colors-input --property "parse.key=true" --property "key.separator=:"

**In such variant of data, Alex/Gogo/Rrr etc are keys**

>Alex:red 
>
>Gogo:blue
>
>Rrr:green
>
>KOKO:asd
>
>KOKO:red

#### Part 2 variant 2 - without keys

**If you don`t have topics yet** 

kafka-topics --bootstrap-server localhost:9092 --topic user-colors-input --create --partitions 3 --replication-factor 1 

kafka-topics --bootstrap-server localhost:9092 --topic user-colors-output --create --partitions 3 --replication-factor 1 

**Consumer/Producer** 

kafka-console-consumer.bat --bootstrap-server localhost:9092 ^    --topic user-colors-output ^    --from-beginning ^    --formatter kafka.tools.DefaultMessageFormatter ^    --property print.key=true ^    --property print.value=true ^    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer ^    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

kafka-console-producer --bootstrap-server localhost:9092 --topic user-colors-input

**In such variant of data there are no keys and messages are just values, 
however in messages we simulate keys(name)**

>Mark,red
>Alex,blue
>Bob,asd
>Steve,red
