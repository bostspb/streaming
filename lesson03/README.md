## Урок 3. Spark Streaming. Чтение Kafka
> Прочитать и разобрать скрипт из урока.
> Создать топик и записать данные в него из файла 1000_orders.json (инструкция ниже).<br>
> Выполнить скрипт на кластере, убедиться что данные отображаются.<br>
> В качестве результата выложить скриншоты с результатами чтения и преобразования данных из Kafka.<br><br>
> Запуск утилиты для записи ордеров в топик Kafka.
> 1. Перед запуском нужно создать свой топик для ордеров.
> 2. Выполнить следующую команду (нужно запускать из той же директории, что и утилита и .json файл):<br>
> `python3.7 orders_data_uploader.py 1000_orders.json bigdataanalytics2-worker-shdpt-v31-1-0:6667 $YOUR_ORDERS_TOPIC_JSON_NAME`

Перекидываем рабочие файлы на головную машину - `scp -r ./ student782_3@37.139.32.56:~`

Потом перекидываем файлы на рабочую машину - `scp -r ./ student782_3@10.0.0.19:~`

Заходим на рабочую машину - `ssh student782_3@37.139.32.56` -> `ssh 10.0.0.19`

Запускаем создание топика:

    /usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --create --topic lesson3_student782_3_topic --zookeeper bigdataanalytics2-worker-shdpt-v31-1-4:2181 --partitions 3 --replication-factor 2 --config retention.ms=17280000000

Записываем данные в топик при помощи утилиты:

    python3.7 orders_data_uploader.py 1000_orders.json bigdataanalytics2-worker-shdpt-v31-1-0:6667 lesson3_student782_3_topic

Запускаем **PySpark**    
    
    /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7

Прогоняем основные фрагменты скрипта `kafka_source.py` - [результат можно посмотреть здесь](https://github.com/bostspb/streaming/blob/master/lesson03/stdout_spark.txt).
