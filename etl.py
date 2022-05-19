from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
c_file = "s3://bdx-emr/ukb49672.csv"
cdata = spark.read.csv(c_file,header='true',inferSchema='true')
v_file = "s3://bdx-emr/lifestyling.csv"
vdata = spark.read.csv(v_file,header='true')
import re
allfields=cdata.columns
rnm_fields = [re.sub(r'[-.]', 'x', field) for field in allfields]
#print(rnm_fields)
ncdata=cdata.toDF(*rnm_fields)
allfields=ncdata.columns
lifefields=vdata.select('Column1')
lifefields_ls=lifefields.rdd.flatMap(lambda x: x).collect()
col2type=dict(ncdata.dtypes)
lifefields_set=set(lifefields_ls)
n=len(allfields)
sel_fields=["eid","31x0x0","21000x0x0","21022x0x0","21001x0x0"]
for i in range(n):
    if allfields[i].split("x")[0] in lifefields_set:
        if col2type[allfields[i]] in ["int","double"]:
            sel_fields.append(allfields[i])
print(len(lifefields_set))
life_data=ncdata.select(sel_fields)
from pyspark.sql.functions import col,isnan, when, count
nullstat=ncdata.select([count(when(col(c).isNull(), c)).alias(c) for c in ncdata.columns])
pandasDF = nullstat.toPandas()
print(pandasDF)
