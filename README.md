# Coursera-Introduction_to_Big_Data_with_Spark_and_Hadoop-Final_Project_Data_Analysis_using_Spark

Task 1: Generate DataFrame from CSV data

employees_df = spark.read.option("header", True).csv("employees.csv")
employees_df.show(5)

Task 2: Define a schema for the data

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("Emp_No", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Department", StringType(), True),
    StructField("Salary", DoubleType(), True)
])

employees_df = spark.read.csv("employees.csv", header=True, schema=schema)
employees_df.show(5)

Task 3: Display schema of DataFrame

employees_df.printSchema()

Task 4: Create a temporary view

employees_df.createOrReplaceTempView("employees")

Task 5: Execute an SQL query (Age > 30)

result_age = spark.sql("SELECT * FROM employees WHERE Age > 30")
result_age.show()

Task 6: Calculate Average Salary by Department

avg_salary = spark.sql("SELECT Department, AVG(Salary) AS Avg_Salary FROM employees GROUP BY Department")
avg_salary.show()

Task 7: Filter and Display IT Department Employees

it_employees = employees_df.filter(employees_df.Department == "IT")
it_employees.show()

Task 8: Add 10% Bonus to Salaries

from pyspark.sql.functions import col

employees_df = employees_df.withColumn("SalaryAfterBonus", col("Salary") * 1.1)
employees_df.show()

Task 9: Find Maximum Salary by Age

from pyspark.sql.functions import max

max_salary_age = employees_df.groupBy("Age").agg(max("Salary").alias("Max_Salary"))
max_salary_age.show()

Task 10: Self-Join on Employee Data

self_join_df = employees_df.alias("e1").join(employees_df.alias("e2"), col("e1.Emp_No") == col("e2.Emp_No"))
self_join_df.show(5)

Task 11: Calculate Average Employee Age

from pyspark.sql.functions import avg

avg_age = employees_df.agg(avg("Age").alias("Average_Age"))
avg_age.show()

Task 12: Calculate Total Salary by Department

from pyspark.sql.functions import sum

total_salary = employees_df.groupBy("Department").agg(sum("Salary").alias("Total_Salary"))
total_salary.show()

Task 13: Sort Data by Age and Salary

sorted_df = employees_df.orderBy(col("Age").asc(), col("Salary").desc())
sorted_df.show()

Task 14: Count Employees in Each Department

from pyspark.sql.functions import count

count_dept = employees_df.groupBy("Department").agg(count("Emp_No").alias("Employee_Count"))
count_dept.show()

Task 15: Filter Employees with the letter 'o' in the Name

employees_with_o = employees_df.filter(col("Name").contains("o"))
employees_with_o.show()

