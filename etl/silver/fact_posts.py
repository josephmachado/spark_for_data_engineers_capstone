import logging
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

def get_upstream_data(spark):
    logger.info("Getting Upstream Data")
    return {
        'post_type': spark.read.table("stackoverflow.post_type"),
        'posts': spark.read.table("stackoverflow.posts"),
    }

def transform_upstream_data(spark, upstream_data):
    logger.info("Transforming Upstream Data")
    return spark.sql("select p.*, pt.postType from {posts} p left join {post_type} pt on p.PostTypeId = pt.id", posts = upstream_data['posts'], post_type = upstream_data['post_type'])

def load_table_data(spark, table_df, table_name):
    logger.info(f"Loading Data into {table_name}")
    spark.sql(f"drop table if exists stackoverflow.{table_name}")
    table_df.write.mode("overwrite").saveAsTable(f"stackoverflow.{table_name}")

def run(spark, table_name="fact_posts"):
    upstream_data = get_upstream_data(spark)
    transformed_data = transform_upstream_data(spark, upstream_data)
    load_table_data(spark, transformed_data, table_name)
    
if __name__ == '__main__':

    executor_memory = "8g"
    executor_cores = 4
    num_executors = 2
    table_name = "fact_posts"

    spark = SparkSession.builder.appName(table_name).config("spark.executor.memory", executor_memory).config("spark.executor.cores", executor_cores).config("spark.executor.instances", num_executors).config("spark.cores.max", executor_cores * num_executors).getOrCreate()

    run(spark, table_name)
