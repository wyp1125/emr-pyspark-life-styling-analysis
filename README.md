# emr-pyspark-life-styling-analysis

<h2>PCA and linear regression analyses for a large dataset using PySpark on AWS EMR</h2>

<h3>Yupeng Wang, Ph.D.</br>
Data science consultant</h3>

<h3>Project summary</h3>

This demo describes the PySpark analysis part in one of my consulting projects. The task was to process and analyze a dataset containing health information of >400k patients and >10k variables (a file size of 24GB). The goal was to obtain a conclusion whether there is an association between thousands of life styling variables and body mass index. This is a typical big data analysis. I determined to utilize PySpark on AWS EMR to realize it.

<h3>Input files</h3>

•	ukb49672.csv: contains health information of 14,939 variables (columns) for 502,415 participants. </br>
•	lifestyling.csv: contains a list of 461 life styling variables to focus on

<h3>Challenges and solutions</h3>
•	The number of analyzed variables was huge. PCA analysis was adopted to reduce the dimensionality of the dataset. </br>
•	List of life styling variables were not exact matches of column names of the main dataset. In addition, column names contain “-” and “.”. Thus, processing of column names was performed. </br>
•	Variables had different proportions of missing values. Variables with high proportions of missing values were first filtered out. Then, NA rows were removed.
