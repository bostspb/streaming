## Урок 7. Spark ML. Аналитика признаков в пакетном режиме. Подготовка, обучение ML-модели
> - Перенести скрипты на рабочую машину и запустить через spark-submit.
> - Ознакомиться с параметрами spark-submit команды.
> - (Опционально): Ознакомиться с Apache Airflow.

Перекидываем скрипты на рабочую машину и заходим на нее
    
    scp -r ./ student782_3@37.139.32.56:~
    ssh student782_3@37.139.32.56
    scp -r ./ student782_3@10.0.0.19:~
    ssh 10.0.0.19

Создаем ресурсные директории в HDFS

    hdfs dfs -mkdir process_csv_as_stream
    hdfs dfs -mkdir tmp/file_output
    hdfs dfs -mkdir tmp/checkpoint

Перекидываем файл с сырыми данными

    hdfs dfs -put ./raw_csv_data.csv process_csv_as_stream/

Запускаем через Spark Submit скрипт `7.1.spark_submit_batch.py` 
([вывод в консоль](https://github.com/bostspb/streaming/blob/master/lesson07/7.1.spark_submit_batch_stdout.txt))
    
    /spark2.4/bin/spark-submit 7.1.spark_submit_batch.py

Проверяем, выходную директорию на наличие выгруженных данных

    [student782_3@bigdataanalytics2-worker-shdpt-v31-1-0 ~]$ hdfs dfs -ls tmp/file_output
    Found 1 items
    drwxr-xr-x   - student782_3 hdfs          0 2021-08-17 05:27 tmp/file_output/p_date=20210817052721
    [student782_3@bigdataanalytics2-worker-shdpt-v31-1-0 ~]$ hdfs dfs -ls tmp/file_output/p_date=20210817052721
    Found 2 items
    -rw-r--r--   2 student782_3 hdfs          0 2021-08-17 05:27 tmp/file_output/p_date=20210817052721/_SUCCESS
    -rw-r--r--   2 student782_3 hdfs      13139 2021-08-17 05:27 tmp/file_output/p_date=20210817052721/part-00000-a1eb6d40-f697-480f-9f05-0b12d5247bae-c000.snappy.parquet

Запускаем через Spark Submit скрипт `7.2.spark_submit_finite_stream.py` 
([вывод в консоль](https://github.com/bostspb/streaming/blob/master/lesson07/7.2.spark_submit_finite_stream_stdout.txt))
    
    /spark2.4/bin/spark-submit 7.2.spark_submit_finite_stream.py

Проверяем, выходную директорию на наличие выгруженных данных

    [student782_3@bigdataanalytics2-worker-shdpt-v31-1-0 ~]$ hdfs dfs -ls tmp/file_output
    Found 2 items
    drwxr-xr-x   - student782_3 hdfs          0 2021-08-17 05:27 tmp/file_output/p_date=20210817052721
    drwxr-xr-x   - student782_3 hdfs          0 2021-08-17 05:38 tmp/file_output/p_date=20210817053843
    [student782_3@bigdataanalytics2-worker-shdpt-v31-1-0 ~]$ hdfs dfs -ls tmp/file_output/p_date=20210817053843
    Found 1 items
    drwxr-xr-x   - student782_3 hdfs          0 2021-08-17 05:38 tmp/file_output/p_date=20210817053843/_spark_metadata
   

Запускаем через Spark Submit скрипт `7.3.spark_submit_infinite_stream.py` 
([вывод в консоль](https://github.com/bostspb/streaming/blob/master/lesson07/7.3.spark_submit_infinite_stream_stdout.txt))
    
    /spark2.4/bin/spark-submit 7.3.spark_submit_infinite_stream.py

Проверяем, выходную директорию на наличие выгруженных данных

    [student782_3@bigdataanalytics2-worker-shdpt-v31-1-0 ~]$ hdfs dfs -ls tmp/file_output
    Found 3 items
    drwxr-xr-x   - student782_3 hdfs          0 2021-08-17 05:27 tmp/file_output/p_date=20210817052721
    drwxr-xr-x   - student782_3 hdfs          0 2021-08-17 05:38 tmp/file_output/p_date=20210817053843
    drwxr-xr-x   - student782_3 hdfs          0 2021-08-17 05:40 tmp/file_output/p_date=20210817054047
    [student782_3@bigdataanalytics2-worker-shdpt-v31-1-0 ~]$ hdfs dfs -ls tmp/file_output/p_date=20210817054047
    Found 2 items
    -rw-r--r--   2 student782_3 hdfs          0 2021-08-17 05:40 tmp/file_output/p_date=20210817054047/_SUCCESS
    -rw-r--r--   2 student782_3 hdfs      13139 2021-08-17 05:40 tmp/file_output/p_date=20210817054047/part-00000-b520c7da-0d20-4d50-8dca-089bfb6bc1c5-c000.snappy.parquet
