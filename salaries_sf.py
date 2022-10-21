from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf,lit

spark = SparkSession.builder.appName("Salaries_Sf").getOrCreate()

salarydf = spark.read.json('/home/saurav/Desktop/SparkAssignment/Salaries.json')
salarydf.printSchema()


# Q1 Find the average salaries of each job position(assuming salary is the total of all pays).
salarydf.groupBy('JobTitle').agg({'TotalPay':'mean'}).show()



# Q2 Which job title has the highest full time employees?
title_and_pay = salarydf.select(salarydf.JobTitle,salarydf.OtherPay)
grouping_jobTitle = title_and_pay.groupBy('JobTitle').agg({'OtherPay':'max'})
in_desc = grouping_jobTitle.orderBy(grouping_jobTitle['max(otherPay)'].desc()).show(1)
# max_salary = grouping_jobTitle.agg({'max(OtherPay)':'max'}).show()


# Q3 List the name of employees who work for the police department? 
def remove_front_char(jobtitle):
    return jobtitle.replace('(','')

remove_char_udf = udf(remove_front_char)  #defining udf for removing '('

def remove_rear_char(jobtitle):
    return jobtitle.replace(')','')

remove_rear_udf= udf(remove_rear_char)    #defining udf for removing ')'

filter1=salarydf.select(salarydf['EmployeeName'],remove_char_udf(salarydf['JobTitle'])).withColumnRenamed('remove_front_char(JobTitle)','JobTitle')
filter2=filter1.select(filter1['EmployeeName'],remove_rear_udf(filter1['JobTitle'])).withColumnRenamed('remove_rear_char(JobTitle)','JobTitle')


split_dept=filter2.select(filter2.EmployeeName,filter2.JobTitle).withColumn('JobTitle',F.explode(F.split('JobTitle',',')))    #splitting and exploding JobTitle having comma
split_dept1=split_dept.select(split_dept.EmployeeName,split_dept.JobTitle).withColumn('JobTitle',F.explode(F.split("JobTitle"," ")))   #exploding and splitting JobTitle having whitespaces


split_white_space = split_dept1.filter((split_dept1.JobTitle=='POLICE'))

final_result = split_white_space.select(split_white_space.EmployeeName).withColumn("JobTitle",lit("POLICE DEPARTMENT")).show()



# Q4 Find the job titles along with the employees name and ids.
salarydf.select(salarydf['JobTitle'],salarydf['EmployeeName'],salarydf['Id']).show()    


# Q5 Find the number of employees in each job title. 
salarydf.groupBy('JobTitle').count().show()


# Q6 List out the names and positions of employees whose total pay is greater than 180000.
salary_filter = salarydf.filter(salarydf['TotalPay']>180000)
result = salary_filter.select(salary_filter['EmployeeName'],salary_filter['JobTitle'],salary_filter['TotalPay']).show()


# Q7 List the names and ids of employees who have never done overtime.
overtime_filter = salarydf.filter(salarydf['OvertimePay']==0)
result = overtime_filter.select(overtime_filter['EmployeeName'],overtime_filter['Id']).show()


