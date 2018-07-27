from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# import pyspark class Row from module sql
from pyspark.sql import *

# Create Example Data - Departments and Employees

# Create the Departments
#department1 = Row(id='123456', name='Computer Science')
#department2 = Row(id='789012', name='Mechanical Engineering')
#department3 = Row(id='345678', name='Theater and Drama')
#department4 = Row(id='901234', name='Indoor Recreation')

# Create the Employees
Employee = Row("firstName", "lastName", "email", "salary")
employee1 = Employee('Khushal', 'Shah', 'khushal@umkc.edu', 100000)
employee2 = Employee('Zeenat', 'Tariq', 'zeenat@umkc', 120000)
employee3 = Employee('Lee', 'Yugyung', 'yugyunglee@umkc.edu', 140000)
employee4 = Employee('Mayanka', 'Chandra', 'Mayanka', 160000)

# Create the DepartmentWithEmployees instances from Departments and Employees
departmentWithEmployees1 = Row(department=department1, employees=[employee1, employee2])
departmentWithEmployees2 = Row(department=department2, employees=[employee3, employee4])
departmentWithEmployees3 = Row(department=department3, employees=[employee1, employee4])
departmentWithEmployees4 = Row(department=department4, employees=[employee2, employee3])

print(department1)
print(employee2)
print(departmentWithEmployees1.employees[0].email)


departmentsWithEmployeesSeq1 = [departmentWithEmployees1, departmentWithEmployees2]
df1 = spark.createDataFrame(departmentsWithEmployeesSeq1)
print("\nDataFrame: 1")
df1.show()

departmentsWithEmployeesSeq2 = [departmentWithEmployees3, departmentWithEmployees4]
df2 = spark.createDataFrame(departmentsWithEmployeesSeq2)
print("\nDataFrame: 2")
df2.show()

print("\nUnion of DataFrame 1 & DataFrame 2")
unionDF = df1.unionAll(df2)
unionDF.show()

print("\nSaved Data to File")
#dbutils.fs.rm("/home/khushal/PycharmProjects/Pyspark/myFile.parquet", True)
unionDF.write.parquet("/home/khushal/PycharmProjects/Pyspark/myFile1.parquet")

print("\nRead Data from File")
parquetDF = spark.read.parquet("/home/khushal/PycharmProjects/Pyspark/myFile1.parquet")
parquetDF.show()






