from pyspark.sql import SparkSession
from pyspark.sql.functions import(
    col,
    expr,
    split,
    explode
)

print("🔥 Python file is executing")

def create_spark_session():
    return SparkSession.builder \
        .appName("PokemonAnalytics") \
        .getOrCreate()


def load_data(spark):
    return spark.read.csv(
        "pokemon-spark/data/processed/pokemon_processed.csv",
        header=True,
        inferSchema=True
    )

def transform_data(df):
    # select only columns needed for the analytical process
    df_selected = df.select(
        col("id"),
        col("name"),
        col("hp"),
        col("attack"),
        col("defense"),
        col("special_attack"),
        col("special_defense"),
        col("speed"),
        col("types")
    )

    # create new dataframe consists of the columns in df_selected 
    # + one new column of "total_stats" contains total base stats of the pokemons
    df_with_total = df_selected.withColumn(
        "total_stats",
        expr(
            "hp + attack + defense + special_attack + special_defense + speed"
        )
    )

    # create new rows for each pokemon having more than 1 type
    df_exploded = df_with_total.withColumn(
        "type",
        explode(split(col("types"), ","))
    )

    return df_exploded

def aggregate_data(df):
    return df.groupBy("type") \
        .avg("total_stats") \
        .withColumnRenamed("avg(total_stats)", "avg_total_stats") \
        .orderBy(col("avg_total_stats").desc())

def write_output(df):
    df.write \
        .mode("overwrite") \
        .parquet("data/output/pokemon_type_stats")
    
def main():
    print("✅ Entered main()")
    spark = create_spark_session()

    df_raw = load_data(spark)
    df_transformed = transform_data(df_raw)
    df_aggregated = aggregate_data(df_transformed)

    df_aggregated.show()
    write_output(df_aggregated)

    spark.stop()


if __name__ == "__main__":
    main()