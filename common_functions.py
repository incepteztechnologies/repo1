from pyspark.sql.functions import col, mean, round
from pyspark.sql.types import *
'''
id,name,age,country,salary,department
1,AAA,25,India,80000,IT
2,BBB,25,India,85000,IT
3,CCC,28,Russia,65000,HR
4,DDD,42,UK,68000,HR
5,EEE,26,USA,75000,Sales
6,FFF,41,Canada,95000,IT
7,GGG,45,China,110000,IT
'''
def read_data(spark, data_file):
    # spark dataframe creation
    print("step1: reading data from csv file")
    usr_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("department", StringType(), True)
    ])

    people_df = spark.read.csv(path=data_file, schema=usr_schema)
    return people_df


def filter_data(people_df):
    # spark dataframe transformation
    print("step2: filtering data based on age")
    filtered_df = people_df.filter(col('age') < 40)
    return filtered_df


def process_data(filtered_df):
    # spark dataframe transformation
    print("step3: aggregation of salary by department")
    processed_df = filtered_df.groupBy(col('department')).agg(round(mean(col('salary'))).alias('avg_salary')). \
        orderBy(col('department'))
    return processed_df


def display_data(processed_df):
    # spark dataframe action
    print("step4: displaying computation results")
    processed_df.show()


def store_data(processed_df):
    # spark dataframe action
    print("step5: Storing the results in output")
    processed_df.coalesce(1).write.mode("overwrite").json("file:///home/hduser/pyspark/data/emp_output")


class cls_common_udf:
    def agerange(self, age):
        if (age <= 12):
            return 'Under 12'
        elif (age >= 13 and age <= 19):
            return 'Between 13 and 19'
        elif (age > 19 and age < 65):
            return 'Between 20 and 65'
        elif (age >= 65):
            return 'Over 65'
        else:
            return 'N/A'