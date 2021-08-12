## Урок 5. Spark Streaming. Stateful streams
> - Прочесть документацию (ссылка в материалах).
> - Прочитать скрипт из лекции, попутно выполняя и разбирая происходящее.
> - Приложить результат выполнения в виде pdf/ветки-в-github/документа.

Заходим на рабочую машину - `ssh student782_3@37.139.32.56` -> `ssh 10.0.0.19`

Создаем свой топик `lesson5_student782_3_orders_topic_json` для сырых данных:

    [student782_3@bigdataanalytics2-worker-shdpt-v31-1-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --create --topic lesson5_student782_3_orders_topic_json --zookeeper bigdataanalytics2-worker-shdpt-v31-1-4:2181 --partitions 3 --replication-factor 2 --config retention.ms=17280000000
    WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
    Created topic "lesson5_student782_3_orders_topic_json".

Записываем данные в созданный топик (файл `1000_orders.json` и утилита `orders_data_uploader.py` были загружены на рабочую машину ранее - при выполнении задания к третьему уроку):

    [student782_3@bigdataanalytics2-worker-shdpt-v31-1-0 ~]$ python3.7 orders_data_uploader.py 1000_orders.json bigdataanalytics2-worker-shdpt-v31-1-0:6667 lesson5_student782_3_orders_topic_json
    Orders file path: 1000_orders.json
    Kafka host: bigdataanalytics2-worker-shdpt-v31-1-0:6667
    Kafka topic: lesson5_student782_3_orders_topic_json

Запускаем **PySpark**
    
    /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 --driver-memory 512m --master local[4] --conf spark.sql.shuffle.partitions=20

Дальше поэтапно выполняем примеры из скрипта `stream_join_and_stateful_windows.py`. 

[Вывод в консоль](https://github.com/bostspb/streaming/blob/master/lesson05/stdout_console.txt)