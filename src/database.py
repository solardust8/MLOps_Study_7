class Database:
    def __init__(self, spark, host="127.0.0.1", port=5432, database='mlops'):
        self.jdbcUrl = f"jdbc:postgresql://{host}:{port}/{database}"
        self.username = "solar"
        self.password = "somepass"
        self.spark = spark
    
    def read_table(self, tablename: str):
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbcUrl) \
            .option("user", self.username) \
            .option("password", self.password) \
            .option("dbtable", tablename) \
            .option("driver", "org.postgresql.Driver") \
            .load()
    
    def insert_df(self, df, tablename):
        df.write \
            .format("jdbc") \
            .option("url", self.jdbcUrl) \
            .option("user", self.username) \
            .option("password", self.password) \
            .option("dbtable", tablename) \
            .mode("append") \
            .save()