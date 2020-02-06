from loadings import *

class retention:
    
    query = ""
   
    def __init__(self, event=("",""), start="", end=""):
        self.event = event
        self.start = start
        self.end = end
    
   
     
    def raw_retention(self):

        self.query=""
        
        if (self.event[1]!=""):
            
            self.query="where "+event[1]
         
        else:
            
            self.query=""
        
        previous_period=(datetime.datetime.strptime(self.start, "%Y-%m-%d") -  timedelta(days=180)).strftime("%Y-%m-%d")
 
        getEvent(previous_period ,self.end,self.event[0]).registerTempTable("second")
        
        getEvent(self.start,self.end,self.event[0]).registerTempTable("second2")
        
        old_users=spark.sql(""" select * from second  {} """.format(self.query)).\
                      select(col("device_id"),to_date(col("timestamp")).alias("effect_date")).\
                      withColumn("effect_name",lit(self.event[0])).distinct().\
                      groupBy("device_id","effect_name").agg(min("effect_date").alias("min_effect_date"))
        
        fresh_users=spark.sql(""" select * from second2  {} """.format(self.query)).\
                        select(col("device_id").alias("dev1"),to_date(col("timestamp")).alias("action_date")).\
                        withColumn("e1",lit(self.event[0])).distinct()
        
        retained_event=spark.sql(""" select * from second2  {}""".format(self.query)).\
                       select(col("device_id").alias("dev2"),to_date(col("timestamp")).alias("event_date")).\
                       withColumn("e2",lit(self.event[0])).distinct()
        
        initial_cohort=fresh_users.alias("a").join(old_users.alias("b"),((col("a.dev1")==col("b.device_id")) & (col("a.e1")==col("b.effect_name"))),"left").\
                       where((col("b.device_id").isNull()) | (col("action_date")==col("min_effect_date")) ).drop("b.device_id")
        
        retention=initial_cohort.alias("a").join(retained_event.alias("b"),(col("a.dev1")==col("b.dev2")) & (col("a.e1")==col("b.e2")),"left").\
                  withColumn("retain_date",datediff(col("event_date"),col("action_date"))).\
                  where(col("retain_date")>=0)
        
        return retention
     
    def retained_days(self):
        return self.raw_retention().\
        where(col("retain_date")>0).\
        groupBy("device_id","action_date","e1").\
        agg(array_sort(collect_list(col("retain_date"))).alias("retain_days")).\
        withColumnRenamed("e1","action_name")
    
    def retention_agg(self):
        return self.raw_retention().\
        groupBy("action_date","retain_date","e1").\
        agg(countDistinct("dev2").alias("retained_count")).\
        withColumnRenamed("e1","action_name").\
        orderBy(col("retain_date")).\
        groupBy(col("action_date")).\
        pivot("retain_date").\
        agg(first(col("retained_count"))).\
        orderBy(col("action_date")).na.fill("")
    
    def retention_agg_weekly(self):
        return self.raw_retention().\
        withColumn("min_date",to_date(lit(self.start))).\
        withColumn("initial_week",concat(lit("week"),(datediff(col("action_date"),to_date(col("min_date")))/7).cast("Integer"))).\
        withColumn("retain_week",concat(lit("week"),(datediff(to_date(col("event_date")),to_date(col("action_date")) )/7).cast("Integer"))).\
        groupBy("initial_week","retain_week","e1").\
        agg(countDistinct("dev2").alias("retained_count")).\
        withColumnRenamed("e1","action_name").\
        orderBy(col("retain_week")).\
        groupBy("initial_week").\
        pivot("retain_week").\
        agg(first(col("retained_count"))).\
        orderBy(col("initial_week")).na.fill("")
