# /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("coursework_student782_3_app").master("local[*]").getOrCreate()

data_path = "coursework/ml_data/loan_data.csv"
model_dir = "coursework/ml_data/models"


# read data from storage

"""
credit.policy:      1 если клиент соответствует критериям кредитного андеррайтинга LendingClub.com, и 0 в противном случае.
purpose:            Цель кредита (принимает значения "creditcard", "debtconsolidation", "educational", "majorpurchase", "smallbusiness" and "all_other").
int.rate:           Процентная ставка по кредиту.
installment:        Ежемесячный платеж
log.annual.inc:     Годовой доход заемщика.
dti:                Отношение долга к доходу заемщика.
fico:               Кредитный рейтинг заемщика FICO.
days.with.cr.line:  Количество дней, в течение которых заемщик имел кредитную линию.
revol.bal:          Возобновляемый баланс заемщика (сумма, не выплаченная в конце платежного цикла кредитной карты).
revol.util:         Коэффициент использования возобновляемой кредитной линии заемщика (сумма использованной кредитной линии по отношению к общей сумме доступного кредита).
inq.last.6mths:     Количество запросов [кредитной истории] заемщика кредиторами за последние 6 месяцев.
delinq.2yrs:        Количество раз, когда заемщик просрочил платеж более чем на 30 дней за последние 2 года.
pub.rec:            Количество негативных записей заемщика: заявления о банкротстве, налоговые залоги или судебные решения.
not.fully.paid:     1 если клиент не выплатил полностью кредит, иначе 0
"""

schema = StructType() \
    .add("credit_policy", IntegerType()) \
    .add("purpose", StringType()) \
    .add("int_rate", DoubleType()) \
    .add("installment", DoubleType()) \
    .add("log_annual_inc", DoubleType()) \
    .add("dti", DoubleType()) \
    .add("fico", IntegerType()) \
    .add("days_with_cr_line", DoubleType()) \
    .add("revol_bal", IntegerType()) \
    .add("revol_util", DoubleType()) \
    .add("inq_last_6mths", IntegerType()) \
    .add("delinq_2yrs", IntegerType()) \
    .add("pub_rec", IntegerType()) \
    .add("not_fully_paid", IntegerType())

data = spark\
    .read\
    .format("csv")\
    .schema(schema) \
    .options(inferSchema=True, header=True) \
    .load(data_path)

data.show(3)
print(data.schema.json())

# target
target = ["not_fully_paid"]

# model evaluator
evaluator = BinaryClassificationEvaluator() \
            .setLabelCol("label") \
            .setMetricName("areaUnderROC")


def prepare_data(data, features, target):
    # features
    f_columns = ",".join(features).split(",")
    # target
    f_target = ",".join(target).split(",")
    f_target = list(map(lambda c: F.col(c).alias("label"), f_target))
    # all columns
    all_columns = ",".join(features + target).split(",")
    all_columns = list(map(lambda c: F.col(c), all_columns))
    # model data set
    model_data = data.select(all_columns).dropna()
    # prepare categorical columns
    if 'purpose' in f_columns:
        text_columns = ['purpose']
        output_text_columns = [c + "_index" for c in text_columns]
        for c in text_columns:
            string_indexer = StringIndexer(inputCol=c, outputCol=c + '_index').setHandleInvalid("keep")
            model_data = string_indexer.fit(model_data).transform(model_data)
        print(model_data.schema.simpleString())
        model_data.show(3)
        # update f_columns with indexed text columns
        f_columns = list(filter(lambda c: c not in text_columns, f_columns))
        f_columns += output_text_columns
    # preparation
    assembler = VectorAssembler(inputCols=f_columns, outputCol='features')
    model_data = assembler.transform(model_data)
    model_data = model_data.select('features', f_target[0])
    model_data.show(3)
    return model_data


def prepare_and_train(data, features, target):
    model_data = prepare_data(data, features, target)
    # train, test
    train, test = model_data.randomSplit([0.8, 0.2], seed=12345)
    # model
    lr = LogisticRegression(featuresCol='features', labelCol='label', maxIter=10, regParam=0.01)
    # train model
    model = lr.fit(train)
    # check the model on the test data
    prediction = model.transform(test)
    prediction.show(10)
    evaluation_result = evaluator.evaluate(prediction)
    print("Evaluation result: {}".format(evaluation_result))
    return model



# MODEL #
features = ["credit_policy,purpose,int_rate,installment,log_annual_inc,dti"]
model = prepare_and_train(data, features, target)
model.write().overwrite().save(model_dir + "/model_best")
