
# coding: utf-8

# In[1]:


#sc
from pyspark.sql.functions import initcap ,col,lit, to_date, months_between


# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf


# In[3]:


sc= SparkSession.builder.appName('MyfirstProgram').getOrCreate()


# In[4]:


player=sc.read.format('csv').option("header","true").load('player.csv')


# In[5]:


player


# In[6]:


player.createOrReplaceTempView("some_sql_view") 


# In[7]:


player


# In[7]:


sc.read.format('csv').option("header","true").load('player.csv').createOrReplaceTempView("some_sql_view") 


# In[8]:


player


# In[8]:


spark.sql("""show databases""").show()


# In[10]:


spark.sql("""create database Test""")


# In[11]:


spark.sql("""show databases""").show()


# In[12]:


player.write.mode("overwrite").saveAsTable("monika.testTable")


# In[14]:


#spark.sql("""use monika""").show()
spark.sql("""show tables """).show()
#spark.sql("""drop table some_sql_view""")


# In[22]:


spark.sql("""describe extended testtable""")


# In[23]:


spark.sql("""show tables """).show()


# In[24]:


spark.catalog


# In[37]:


spark.sql("""drop table Player_replica""")
spark.sql("""create table Player_replica 
(id string,
player_api_id string,
player_name string,
player_fifa_api_id string,
birthday string,
height string,
weight string
)""")


# In[36]:


spark.sql("""drop table Player_replica""")


# In[29]:


spark.sql("create table Player_replica (id string,player_api_id string,player_name string,player_fifa_api_id string,birthday string,height string,weight string) USING CSV OPTIONS (path 'player.csv')")


# In[30]:


spark.sql("""drop table Player_replica""")
spark.sql("""create table Player_replica 
(id string,
player_api_id string,
player_name string,
player_fifa_api_id string,
birthday string COMMENT "remember, the US will be most prevalent",
height string,
weight string
)""")


# In[32]:


spark.sql("""describe table Player_replica""")


# In[39]:


#spark.sql("""drop table Player_replica""")
spark.sql("""create table Player_replica1
(player_api_id string,
player_name string,
player_fifa_api_id string,
birthday string COMMENT "remember, the US will be most prevalent",
height string,
weight string
) """)


# In[44]:


spark.sql("""EXPLAIN SELECT * FROM Player_replica1""").show()


# In[45]:


spark.sql("""describe table Player_replica1""").show()


# In[42]:


spark.sql("""SELECT current_database()""").show()


# In[18]:


spark.sql("""select * from Player_replica""").show(1)


# In[19]:


spark.sql("""SELECT (player_api_id, player_fifa_api_id) as unique_id 
FROM Player_replica""").show(11)

SHOW FUNCTIONS


# In[22]:


spark.sql("""SHOW FUNCTIONS""").show()


# In[34]:


#spark.sql("""SHOW SYSTEM FUNCTIONS""").show(5)
#spark.sql("""SHOW USER FUNCTIONS""").show(5)
#spark.sql('''SHOW FUNCTIONS "s*"''').show(5)
#spark.sql('''SHOW FUNCTIONS LIKE "collect*"''').show(5)


# In[47]:


#spark.sql("""SELECT collect_list(player_api_id) as unique_id FROM Player_replica group by player_api_id""").show(11)

spark.sql("""SELECT (player_api_id, player_fifa_api_id) as unique_id , ARAAY(1,2,3) FROM Player_replica group by player_api_id""").show(11)


# In[25]:


a=spark.sql("""select * from some_sql_view""").count()


# In[9]:


spark.sql("""SELECT DEST_COUNTRY_NAME, sum(count) FROM some_sql_view GROUP BY DEST_COUNTRY_NAME """)\  .where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10").count() # SQL => DF


# In[32]:


A=spark.sql('''CREATE TABLE flights1 USING CSV OPTIONS (path 'player.csv')''')


# In[33]:


A.show(5)


# In[8]:


player.select(to_date(lit("2019-01-01")).alias("start"),to_date(lit("2017-05-22")).alias("end")).select(months_between(col("start"),col("end"))).show(1)


# In[6]:


#player.printSchema()


# In[7]:


player.schema
#player.show(5)
#player.select('player_name').filter(player_name=='Aaron Doran').collect()


# In[45]:


player.count()


# In[8]:


player.show(2)


# In[21]:


player.select(initcap(lit("abc sh  gchs vsd gsh"))).show()


# In[23]:


from pyspark.sql.functions import lower, upper
player.select(lit("Hello everyone"),    lower(lit("Hello everyone")),    upper(lit("Hello everyone"))).show(2)


# In[74]:


#player.select('id','player_api_id','player_name','height').show(5)
players=player.drop('id','player_fifa_api_id')
players.show(5)


# In[30]:


from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim 
player.select(lit("    HELLO    ").alias("ltrim"),              ltrim(lit("    HELLO    ")).alias("ltrim"),              rtrim(lit("    HELLO    ")).alias("rtrim"),              trim(lit("    HELLO    ")).alias("trim"),              lpad(lit("HELLO"), 3, " ").alias("lp"),              rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)


# In[82]:


#player_attributes=sc.read.format('csv').option("header","true").load('player_attributes.csv')
#player_attributes.select('id','player_api_id','potential').show(1)
#player_att=player_attributes.drop('id','player_fifa_api_id','preferred_foot','attacking_work_rate','attacking_work_rate','crossing','jumping','sprint_speed','balance','aggression','interceptions','short_passing','potential')
#player_att.printSchema()


# In[103]:


#player_attributes.printSchema()  
#player_attributes.columns


# In[79]:


#players
#pop= player.join(player_attributes,player.player_api_id==player_attributes.player_api_id )


# In[115]:


#year_extract_date= udf(lambda data:date.split('-')[0])
#player_attributes=player_attributes.withColumn('year',year_extract_date(player_attributes.date))


# In[1]:


#player_attributes.columns


# #player_att.columns
# #player_att.withColumnRenamed('year','year')

# In[10]:


#player_2016=player_attributes.filter(player_attributes.year==2016)
player_attributes.select(player_2016.player_api_id).distinct().count()


# In[ ]:


players_att_join= players.join(player_att, players)


# In[4]:


player.columns


# In[8]:


player=player.drop('player_fifa_api_id')
player.select('player_api_id').show(5)


# In[46]:


player.schema


# In[50]:


spark.sql("""create table Player_replica2
(id string,
player_api_id string,
player_name string,
player_fifa_api_id string,
birthday string,
height string,
weight string
)""")


# In[54]:


sc.read.format('csv').option("header","true").load('sales.csv').createOrReplaceTempView("sales") 


# In[60]:


spark.sql("""select SalesId,SalesRegion, sum(SalesVolume) from sales GROUP BY CUBE(SalesId,SalesRegion)""").show(10)


# In[58]:


spark.sql("""select SalesId,SalesRegion, sum(SalesVolume) from sales GROUP BY rollup(SalesId,SalesRegion)""").show(10)


# In[59]:


spark.sql("""select SalesId, sum(SalesVolume) from sales GROUP BY CUBE(SalesId,SalesRegion)""").show(10)

