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

import pyspark.sql.functions as F
null_counts = life_data.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in life_data.columns]).collect()[0].asDict()
m=life_data.count()

cut_ratio=0.1
rm_v=0
for key in null_counts:
    if null_counts[key]>cut_ratio*m:
        rm_v+=1
n_v=len(life_data.columns)
print("Number of total variables:"+str(n_v))
print("Number of removed variables:"+str(rm_v))
to_drop = [k for k, v in null_counts.items() if v > cut_ratio*m]
life_data_1 = life_data.drop(*to_drop)

life_data_2=life_data_1.na.drop("any")
m_1=life_data_2.count()
print(m_1)

from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
assembler = VectorAssembler(inputCols = life_data_2.columns[5:], outputCol = 'features')
life_data_3 = assembler.transform(life_data_2).select('features')

scaler = StandardScaler(
    inputCol = 'features', 
    outputCol = 'scaledFeatures',
    withMean = True,
    withStd = True
).fit(life_data_3)
life_data_4 = scaler.transform(life_data_3)

n_components = 3
pca = PCA(
    k = n_components, 
    inputCol = 'scaledFeatures', 
    outputCol = 'pcaFeatures'
).fit(life_data_4)
df_pca = pca.transform(life_data_4)
print('Explained Variance Ratio', pca.explainedVariance.toArray())
