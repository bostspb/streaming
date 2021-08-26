## Урок 8. Spark Streaming + Spark ML + Cassandra. Применение ML-модели в режиме реального времени
> Финальный проект состоит из 2 частей и выглядит следующим образом:
> 
> **Часть 1:**
> 1. Найти датасет для машинного обучения (пользуемся поиском - например датасет про Титаник)
> 2. Реализовать простую предобработку (если есть опыт в ML, то можно сделать более комплексную)
> 3. Обучить модель используя Spark ML (можно использовать Classifier, Validator или Pipeline)
> 4. Выгрузить модель в HDFS
> 
> **Часть 2:**
> 1. Реализовать Spark Streaming приложение, которое читает HDFS/Kafka
> 2. Прочитать обученную модель (выгрузка в части 1 шаг 4) в этом приложении
> 3. Спрогнозировать данные в потоке и выгрузить в Kafka/HDFS/Cassandra
> 
> Вспомогательный код можно найти в скриптах лекции.

Находим на kaggle.com датасет (
[источник](https://www.kaggle.com/itssuru/loan-data), 
[CSV-файл](https://github.com/bostspb/streaming/blob/master/lesson08/coursework/loan_data.csv)
) с данными по займам пользователей ресурса LendingClub.com

Перекидываем файл с данными и скрипт запуска стриминга на рабочую машину и заходим на нее
    
    scp -r ./loan_data.csv student782_3@37.139.32.56:~
    scp -r ./stream.py student782_3@37.139.32.56:~
    ssh student782_3@37.139.32.56
    scp -r ./loan_data.csv student782_3@10.0.0.19:~
    scp -r ./stream.py student782_3@10.0.0.19:~
    ssh 10.0.0.19

Создаем ресурсные директории в HDFS

    hdfs dfs -mkdir coursework    
    hdfs dfs -mkdir coursework/ml_data
    hdfs dfs -mkdir coursework/ml_data/models
    hdfs dfs -mkdir coursework/ml_data/output
    hdfs dfs -mkdir tmp/coursework_ml_checkpoint

Перекидываем файл с данными

    hdfs dfs -put ./loan_data.csv coursework/ml_data/

Проверяем, что файл с данными на месте

    [student782_3@bigdataanalytics2-worker-shdpt-v31-1-0 ~]$ hdfs dfs -ls coursework/ml_data
    Found 2 items
    -rw-r--r--   2 student782_3 hdfs     751253 2021-08-22 07:19 coursework/ml_data/loan_data.csv
    drwxr-xr-x   - student782_3 hdfs          0 2021-08-22 07:18 coursework/ml_data/models

Обучаем модель, подбираем набор полей, дающий максимальную точность прогноза и сохраняем полученную модель: 
[скрипт train.py](https://github.com/bostspb/streaming/blob/master/lesson08/coursework/train.py),
[лог обучения](https://github.com/bostspb/streaming/blob/master/lesson08/coursework/train_log.txt).

Запускаем стримминг данных с чтением из CSV-файла на HDFS и выгрузкой результирующего прогноза в HDFS в виде файла в формате Parquet (
[скрипт stream.py](https://github.com/bostspb/streaming/blob/master/lesson08/coursework/stream.py),
[вывод](https://github.com/bostspb/streaming/blob/master/lesson08/coursework/stream_log.txt)):

    /spark2.4/bin/spark-submit stream.py

Проверяем, что в HDFS появились файлы с выгрузкой:

    [student782_3@bigdataanalytics2-worker-shdpt-v31-1-0 ~]$ hdfs dfs -ls coursework/ml_data/output
    Found 1 items
    drwxr-xr-x   - student782_3 hdfs          0 2021-08-26 05:36 coursework/ml_data/output/p_date=20210826053634
    [student782_3@bigdataanalytics2-worker-shdpt-v31-1-0 ~]$ hdfs dfs -ls coursework/ml_data/output/p_date=20210826053634
    Found 2 items
    -rw-r--r--   2 student782_3 hdfs          0 2021-08-26 05:36 coursework/ml_data/output/p_date=20210826053634/_SUCCESS
    -rw-r--r--   2 student782_3 hdfs     418143 2021-08-26 05:36 coursework/ml_data/output/p_date=20210826053634/part-00000-ba3afdcb-7056-43d1-8250-5c893998d10b-c000.snappy.parquet
