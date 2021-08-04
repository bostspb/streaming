# GB: Потоковая обработка данных
> **Geek University Data Engineering**

`Spark Streaming` `Kafka` `Cassandra` `Spark ML`

### Урок 1. Spark Streaming. Тестовые стримы, чтение файлов в реальном времени.

[Apache Spark - Structured Streaming](https://spark.apache.org/docs/2.4.7/structured-streaming-programming-guide.html#programming-model)<br>
[Input Source](https://spark.apache.org/docs/2.4.7/structured-streaming-programming-guide.html#input-sources)<br>

**Задание** <br>
- Ознакомиться с документацией по ссылкам в разделе "Комментарии"
- Запустить скрипт на кластере и приложить файлы-скриншоты/pdf-файлы результатов исполнения блоков скрипта
- Запуская скрипт, разобраться в том, как это написано, обрабатывается (операясь на документацию)

**Решение** <br>
[Ход выполнения задания и результирующий вывод в консоль](https://github.com/bostspb/streaming/blob/master/lesson01/README.md)


### Урок 2. Kafka. Архитектура

1. Why Apache Kafka
2. Apache Kafka interchange
3. Apache Kafka organization
    - Broker
    - Zookeeper
    - Producer
    - Topic
    - Partition
    - Offset
    - Consumer, Consumer group
4. Delivery semantics

**Задание** <br>
Подключится к кластеру и выполнить команды из файла lesson2.practice.txt.

**Решение** <br>
[Ход выполнения задания и результирующий вывод в консоль](https://github.com/bostspb/streaming/blob/master/lesson02/README.md)


### Урок 3. Spark Streaming. Чтение Kafka

**Задание** <br>
- Прочитать и разобрать скрипт из урока.
- Создать топик и записать данные в него из файла `1000_orders.json` (инструкция ниже).<br>
- Выполнить скрипт на кластере, убедиться что данные отображаются.<br>
- В качестве результата выложить скриншоты с результатами чтения и преобразования данных из Kafka.<br>
- Запуск утилиты для записи ордеров в топик Kafka.
   1. Перед запуском нужно создать свой топик для ордеров.
   2. Выполнить следующую команду (нужно запускать из той же директории, что и утилита и .json файл):<br>
   `python3.7 orders_data_uploader.py 1000_orders.json bigdataanalytics2-worker-shdpt-v31-1-0:6667 $YOUR_ORDERS_TOPIC_JSON_NAME`
      
**Решение** <br>
[Ход выполнения задания и результирующий вывод в консоль PySpark](https://github.com/bostspb/streaming/blob/master/lesson03/README.md)
