## Урок 6. Lambda архитектура. Spark Streaming + Cassandra
> - Прочесть документацию (ссылка в материалах).
> - Прочитать скрипт из лекции, попутно выполняя и разбирая происходящее.
> - Приложить результат выполнения в виде pdf/ветки-в-github/документа.

Заходим на рабочую машину - `ssh student782_3@37.139.32.56` -> `ssh 10.0.0.19`

Проверяем запущена ли Cassandra

    [student782_3@bigdataanalytics2-worker-shdpt-v31-1-0 ~]$ /cassandra/bin/nodetool status
    Datacenter: datacenter1
    =======================
    Status=Up/Down
    |/ State=Normal/Leaving/Joining/Moving
    --  Address    Load       Tokens       Owns (effective)  Host ID                               Rack
    UN  127.0.0.1  330.8 MiB  256          100.0%            41cf1164-fb96-4b14-b9c1-0f6c4c613a1b  rack1

Запускаем консоль Cassandra

    [student782_3@bigdataanalytics2-worker-shdpt-v31-1-0 ~]$ /cassandra/bin/cqlsh
    Connected to Test Cluster at 127.0.0.1:9042.
    [cqlsh 5.0.1 | Cassandra 3.11.8 | CQL spec 3.4.4 | Native protocol v4]
    Use HELP for help.
    cqlsh>

Дальше в файле `orders.cql` заменяем название **KEYSPACE** с `streaming_1004` на `streaming_student782_3` и
выполняем все инструкции из него в консоли Cassandra - создание **KEYSPACE** и заливку данных.

Выходим из консоли Cassandra.

Создаем свой топик в Kafka `lesson6_student782_3_orders_topic_json` для сырых данных:

    [student782_3@bigdataanalytics2-worker-shdpt-v31-1-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --create --topic lesson6_student782_3_orders_topic_json --zookeeper bigdataanalytics2-worker-shdpt-v31-1-4:2181 --partitions 3 --replication-factor 2 --config retention.ms=17280000000
    WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
    Created topic "lesson6_student782_3_orders_topic_json".

Записываем данные в созданный топик (файл `1000_orders.json` и утилита `orders_data_uploader.py` были загружены на рабочую машину ранее - при выполнении задания к третьему уроку):

    [student782_3@bigdataanalytics2-worker-shdpt-v31-1-0 ~]$ python3.7 orders_data_uploader.py 1000_orders.json bigdataanalytics2-worker-shdpt-v31-1-0:6667 lesson6_student782_3_orders_topic_json
    Orders file path: 1000_orders.json
    Kafka host: bigdataanalytics2-worker-shdpt-v31-1-0:6667
    Kafka topic: lesson6_student782_3_orders_topic_json

Запускаем **PySpark**
    
    /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[2] --conf spark.sql.shuffle.partitions=20

Дальше поэтапно выполняем примеры из скрипта `spark_to_cassandra.py`. 

[Вывод в консоль](https://github.com/bostspb/streaming/blob/master/lesson06/stdout_console.txt)

[Проверяем наличие новой таблицы при заливке через Spark](https://github.com/bostspb/streaming/blob/master/lesson06/stdout_cqlsh.txt)
