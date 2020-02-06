from loadings import *

class Funnel:
    
    join_base = None
    participate = None
    device_base=None
    final_df = None
    countries = spark.read.parquet("/analyst-shared/countries")
    installs=spark.read.parquet("/analytics/entities/installs").\
    where(col("app")=="com.picsart.studio").\
    select(col("device_id").alias("dev"),to_date("timestamp").alias("install_date"))
    query = ""
    partitions = None
    step = 1
    exprs1 = None
    df = None

    def __init__(self, event=[("","")], start="", end="", aggregate="device_id",agg="date,new_old",absolute=True):
        self.event = event
        self.start = start
        self.end = end
        self.aggregate = aggregate
        self.agg=agg
        self.absolute=absolute
    

    def raw_data(self):
        self.join_base = None
        self.device_base = None
        self.final_df = None
        self.step = 1
        self.exprs1 = None
        self.df = None
        partitions = None
   

        if (self.aggregate == "device"):
            self.partitions = Window.partitionBy(col("date1"), col("device_id1"))
        else:
            self.partitions = Window.partitionBy(col("date1"), col("device_id1"), col("session_id1"))

        for d in self.event:
            if(self.step==1):
                if (d[1] != ""):

                    self.query = "where " + d[1]

                else:

                    self.query = ""

                getEvent(self.start, self.end, d[0]).registerTempTable("first")

                initial = spark.sql(""" select * from first  {} """.format(self.query)).\
                          select(col("device_id").alias("device_id" + str(self.step)),
                                 col("platform").alias("platform" + str(self.step)),
                                 to_date("timestamp").alias("date" + str(self.step)),
                                 col("timestamp").alias("timestamp" + str(self.step)),
                                 lower(col("country_code")).alias("country_code" + str(self.step)),
                                 col("session_id").alias("session_id" + str(self.step))
                          ).withColumn(d[0], col("device_id" + str(self.step))).alias("a").\
                          join(self.installs.alias("b"),
                               ((col("a.device_id"+str(self.step))==col("b.dev")) & (col("a.date"+str(self.step))==col("b.install_date"))),"left").\
                          withColumn("new_old",when(col("b.dev").isNull(),"old").otherwise("new")).\
                          drop(col("dev")).drop(col("install_date"))
                self.join_base = initial

            else:
                if (d[1] != ""):

                    self.query = "where " + d[1]

                else:

                    self.query = ""

                getEvent(self.start, self.end, d[0]).registerTempTable("second")
                
                first = spark.sql(""" select * from second  {} """.format(self.query)).\
                select(col("device_id").alias("device_id" + str(self.step)),
                col("platform").alias("platform" + str(self.step)),
                to_date("timestamp").alias("date" + str(self.step)),
                col("timestamp").alias("timestamp" + str(self.step)),
                lower(col("country_code")).alias("country_code" + str(self.step)),
                col("session_id").alias("session_id" + str(self.step))).\
                withColumn(d[0], col("device_id" + str(self.step)))
                
                self.join_base = self.join_base.alias("a").join(first.alias("b"), 
                (col("device_id" + str(self.step - 1)) == col("device_id" + str(self.step))) & 
                (col("session_id" + str(self.step - 1)) == col("session_id" + str(self.step))) & 
                (col("date" + str(self.step - 1)) == col("date" + str(self.step))) & 
                (col("timestamp" + str(self.step - 1)) <= col("timestamp" + str(self.step))), "left").\
                withColumn("rank", row_number().over(self.partitions.orderBy(col("timestamp1"), 
                col("timestamp" + str(self.step))))). \
                where((col("rank") == 1)).drop("rank")
            self.step = self.step + 1

        self.exprs1 = list(map(lambda c: when(col(c[0]).isNotNull(), 1).otherwise(0).alias(c[0]), self.event)) +\
        [col("device_id1"), col("platform1"), col("country_code1"), col("date1"),col("new_old")]
        self.df = self.join_base.withColumn("device_id1", col(self.event[0][0])).\
        select(*self.exprs1).\
        withColumnRenamed("device_id1", "device_id").\
        withColumnRenamed("session_id1", "session_id").\
        withColumnRenamed("platform1","platform").\
        withColumnRenamed("country_code1", "country_code").\
        withColumnRenamed("date1", "date")
        self.final_df = self.df.\
        select("device_id","platform","date","new_old",*[i[0] for i in self.event])

        return self.final_df

    def funnel_agg(self):
  
        exprs_agg_i = [x for x in self.raw_data().columns if
                       x not in ["device_id", "date", "platform", "country_code", "new_old"]]

        exprs_agg_abs = list(
            map(lambda c: round(pyspark.sql.functions.sum(pyspark.sql.functions.col(c)), 1).alias(c), exprs_agg_i))

        exprs_agg_rel = list(map(lambda c: (round(pyspark.sql.functions.sum(pyspark.sql.functions.col(c)), 1) / round(
            pyspark.sql.functions.sum(pyspark.sql.functions.col(self.event[0][0])), 1)).alias(c), exprs_agg_i))

        if (self.absolute):
            return self.raw_data().groupBy(*self.agg.split(",")).agg(*exprs_agg_abs)
        else:
            return self.raw_data().groupBy(*self.agg.split(",")).agg(*exprs_agg_rel)
