## Урок 4. Spark Streaming. Sinks
- Запустить скрипты и предположить, что можно решать на основе разных Sink'ов.
- Обязательно ознакомиться с документацией.
- Опциональным будет запустить эксперименты на уровне Spark API.


Заходим на рабочую машину - `ssh student782_3@37.139.32.56` -> `ssh 10.0.0.19`

Запускаем **PySpark** ([вывод](https://github.com/bostspb/streaming/blob/master/lesson04/001_start_pyspark.txt))
    
    /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 --driver-memory 512m --num-executors 1 --executor-memory 512m --master local[4]

Дальше будем поэтапно прогонять примеры синков из скрипта `spark_sinks.py`. 

Сначала инициализируем приложение ([вывод](https://github.com/bostspb/streaming/blob/master/lesson04/002_init_app.txt)).

Затем запускаем первый синк - **вывод в консоль** ([вывод](https://github.com/bostspb/streaming/blob/master/lesson04/003_sink_via_console.txt)).
Могу предположить, что данный синк используется исключительно для тестирования настройки пайплайна, 
чтобы проверить корректность структуры транспортируемых данных на промежуточных этапах.

Затем пробуем синк **memory** ([вывод](https://github.com/bostspb/streaming/blob/master/lesson04/004_sink_via_memory.txt)).
Мне кажется, что данный синк удобен также для тестирования пайплайна, только еще позволяет на лету трансформировать данные
при необходимости, т.к. сливает данные в память как в некий буфер, из которого мы можем сразу их взять и посмотреть 
с нужной структурой или после небольшой трансформации.

После этого пробуем **фаловый** синк ([вывод](https://github.com/bostspb/streaming/blob/master/lesson04/005_sink_via_file.txt)).

Перед его использованием очищаем целевые директории, куда должны будуть падать файлы с данными.

    hdfs dfs -ls tmp/orders_file_output
    hdfs dfs -ls tmp/orders_checkpoint
    
    hdfs dfs -rm -r tmp/orders_file_output
    hdfs dfs -rm -r tmp/orders_checkpoint
    
    hdfs dfs -mkdir tmp/orders_file_output
    hdfs dfs -mkdir tmp/orders_checkpoint

Но это не помогло - скрипт падает с ошибкой 

    21/08/09 05:04:35 WARN clients.NetworkClient: [Consumer clientId=consumer-1, groupId=spark-kafka-source-29838005-1dd3-4658-b6e4-b6fd3003e3cc-433752199-driver-0] Connection to node -1 could not be established. Broker may not be available.

Файловый синк, как мне кажется, удобен для работы с продуктовым пайплайном загрузки данных как в промежуточном звене, так и конечном.


Дальше опробуем синк через **Kafka** ([вывод](https://github.com/bostspb/streaming/blob/master/lesson04/003_sink_via_kafka.txt)).

Похоже отвалился брокер на всех нодах

exit()