from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql import functions as f

TOPIC_NAME_IN = 'p.mavrichev_in'
TOPIC_NAME_OUT = 'p.mavrichev_out'
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'
}

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# функция чтния потока кампаний от ресторанов
def read_restaurant_stream(spark: SparkSession) -> DataFrame:

    schema = StructType([
        StructField("restaurant_id", StringType(), False),
        StructField("adv_campaign_id", StringType(), False),
        StructField("adv_campaign_content", StringType(), True),
        StructField("adv_campaign_owner", StringType(), True),
        StructField("adv_campaign_owner_contact", StringType(), True),
        StructField("adv_campaign_datetime_start", DoubleType(), True),
        StructField("adv_campaign_datetime_end", DoubleType(), True),
        StructField("datetime_created", DoubleType(), True),
    ])

    df = (spark.readStream.format('kafka')
          .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
          .option("subscribe", TOPIC_NAME_IN)
          .options(**kafka_security_options)
          .option("maxOffsetsPerTrigger", 1000)
          .load()
          .withColumn('value', f.col('value').cast(StringType()))
          .withColumn('event', f.from_json(f.col('value'), schema))
          .selectExpr('event.*')
          .withColumn('adv_campaign_datetime_start',
                      f.from_unixtime(f.col('adv_campaign_datetime_start'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
          .withColumn('adv_campaign_datetime_end',
                      f.from_unixtime(f.col('adv_campaign_datetime_end'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
          .withColumn('datetime_created',
                      f.from_unixtime(f.col('datetime_created'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
          .dropDuplicates(['restaurant_id', 'datetime_created'])
          .withWatermark('datetime_created', '10 minutes')
          .filter((f.col('adv_campaign_datetime_start') <= f.lit(datetime.now())) & (f.col('adv_campaign_datetime_end') >= f.lit(datetime.now())))
          )

    return df

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    # записываем df в PostgreSQL с полем feedback
    df.withColumn('feedback', f.lit(''))\
                .write.format('jdbc')\
                .mode('append')\
                .option('url', 'jdbc:postgresql://localhost:5432/de') \
                .option('driver', 'org.postgresql.Driver') \
                .option('dbtable', 'public.subscribers_feedback') \
                .option('user', 'jovyan') \
                .option('password', 'jovyan')\
                .save()

    # создаём df для отправки в Kafka. Сериализация в json.
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    columns=['restaurant_id','adv_campaign_id',
            'adv_campaign_content', 'adv_campaign_owner',
            'adv_campaign_datetime_start' , 'adv_campaign_datetime_end', 
            'datetime_created', 'trigger_datetime_created']

    df.withColumn('value', f.to_json(f.struct(columns))).select('value')\
            .write\
            .format("kafka")\
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
            .options(**kafka_security_options)\
            .option('topic', TOPIC_NAME_OUT) \
            .save()
    # очищаем память от df
    df.unpersist()

# поток кампаний
restaurant_read_stream_df = read_restaurant_stream(spark)

# подписки клиентов на рестораны
subscribers_restaurant_df = spark.read \
                            .format('jdbc') \
                            .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                            .option('driver', 'org.postgresql.Driver') \
                            .option('dbtable', 'subscribers_restaurants') \
                            .option('user', 'student') \
                            .option('password', 'de-student') \
                            .load()

# объединяем поток команий с датасетом подписок
join_df = restaurant_read_stream_df\
        .join(subscribers_restaurant_df, 'restaurant_id', how='inner')\
        .drop('id')\
        .distinct()\
        .withColumn("trigger_datetime_created", f.lit(datetime.now()))

# запускаем обработку
join_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()


