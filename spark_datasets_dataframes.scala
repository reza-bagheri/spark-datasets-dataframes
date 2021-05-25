// Databricks notebook source
// MAGIC %md
// MAGIC #### Creating an RDD

// COMMAND ----------

val someData = Seq(
  ("John", "Data scientist", 4500),
  ("James", "Data engineer", 3200),
  ("Laura", "Data scientist", 4100),
  ("Ali", "Data engineer", 3200),
  ("Steve", "Developer", 3600)
)
val rdd=spark.sparkContext.parallelize(someData)
rdd.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Encoders

// COMMAND ----------

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

case class MyClass(name: String, id: Int, role: String)
val myClassEncoder = ExpressionEncoder[MyClass]()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### InternalRow and UnsafeRow

// COMMAND ----------

val toRow = myClassEncoder.createSerializer()
toRow(MyClass("John", 7, "developer"))

// COMMAND ----------

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
val internalRow = toRow(MyClass("John", 7, "developer"))
val unsafeRow = internalRow.asInstanceOf[UnsafeRow]
unsafeRow.getBytes

// COMMAND ----------

case class MyClass(name: Option[String], id: Option[Int], role: Option[String])
val myClassEncoder = ExpressionEncoder[MyClass]()
val toRow = myClassEncoder.createSerializer()
val internalRow = toRow(MyClass(Some("John"), Some(7), None))
val unsafeRow = internalRow.asInstanceOf[UnsafeRow]
unsafeRow.getBytes

// COMMAND ----------

// MAGIC %md
// MAGIC #### Creating DataSets

// COMMAND ----------

import org.apache.spark.sql.Encoders

case class Employee(name: String, role: String, salary: Integer)
val employeeEncoder = Encoders.product[Employee]
val data = Seq(Employee("John", "Data scientist", 4500),
               Employee("James", "Data engineer", 3200),
               Employee("Laura", "Data scientist", 4100),
               Employee("Ali", "Data engineer", 3200),
               Employee("Steve", "Developer", 3600))
val staffDS = spark.createDataset(data)(employeeEncoder)
staffDS.show()

// COMMAND ----------

import spark.implicits._
case class Employee(name: String, role: String, salary: Integer)
val data = Seq(Employee("John", "Data scientist", 4500),
               Employee("James", "Data engineer", 3200),
               Employee("Laura", "Data scientist", 4100),
               Employee("Ali", "Data engineer", 3200),
               Employee("Steve", "Developer", 3600))
val staffDS = spark.createDataset(data)
staffDS.show()

// COMMAND ----------

import spark.implicits.newProductEncoder

case class Employee(name: String, role: String, salary: Integer)
val data = Seq(Employee("John", "Data scientist", 4500),
               Employee("James", "Data engineer", 3200),
               Employee("Laura", "Data scientist", 4100),
               Employee("Ali", "Data engineer", 3200),
               Employee("Steve", "Developer", 3600))
val staffDS = spark.createDataset(data)(newProductEncoder[Employee])
staffDS.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Dataframes

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Row objects

// COMMAND ----------

import org.apache.spark.sql.Row
val row = Row("John", "Data scientist", 4500)
row(2)

// COMMAND ----------

row.getAs[String](0)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Schema

// COMMAND ----------

import org.apache.spark.sql.types._
val someSchema = StructType(List(
  StructField("name", StringType, true),
  StructField("role", StringType, true),
  StructField("salary", IntegerType, true))
)

// COMMAND ----------

val someSchema = "`name` STRING, `role` STRING, `salary` INTEGER"

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Creating a DataFrame from a Dataset

// COMMAND ----------

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
val someSchema = StructType(List(
  StructField("name", StringType, true),
  StructField("role", StringType, true),
  StructField("salary", IntegerType, true))
)
val encoder = RowEncoder(someSchema)

val data = Seq(Row("John", "Data scientist", 4500),
               Row("James", "Data engineer", 3200),
               Row("Laura", "Data scientist", 4100),
               Row("Ali", "Data engineer", 3200),
               Row("Steve", "Developer", 3600))
val staffDS = spark.createDataset(data)(encoder)
staffDS.show()

// COMMAND ----------

val newStaffDF: org.apache.spark.sql.DataFrame = staffDS 

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Creating a DataFrame from scratch

// COMMAND ----------

import org.apache.spark.sql.types._
val someData = Seq(
  Row("John", "Data scientist", 4500),
  Row("James", "Data engineer", 3200),
  Row("Laura", "Data scientist", 4100),
  Row("Ali", "Data engineer", 3200),
  Row("Steve", "Developer", 3600)
)

val someSchema = StructType(List(
  StructField("name", StringType, true),
  StructField("role", StringType, true),
  StructField("salary", IntegerType, true))
)

val staffDF = spark.createDataFrame(
  spark.sparkContext.parallelize(someData),
  someSchema
)

staffDF.show()

// COMMAND ----------

val someData = Seq(
  Row("John", "Data scientist", 4500),
  Row("James", "Data engineer", 3200),
  Row("Laura", "Data scientist", 4100),
  Row("Ali", "Data engineer", 3200),
  Row("Steve", "Developer", 3600)
)

val someSchema = "`name` STRING, `role` STRING, `salary` INTEGER"

val staffDF = spark.createDataFrame(
  spark.sparkContext.parallelize(someData),
  StructType.fromDDL(someSchema)
)

staffDF.show()

// COMMAND ----------

val someData = Seq(
  ("John", "Data scientist", 4500),
  ("James", "Data engineer", 3200),
  ("Laura", "Data scientist", 4100),
  ("Ali", "Data engineer", 3200),
  ("Steve", "Developer", 3600)
)

val staffDF = spark.createDataFrame(someData)
staffDF.show()

// COMMAND ----------

import spark.implicits._
val staffDF = Seq(
  ("John", "Data scientist", 4500),
  ("James", "Data engineer", 3200),
  ("Laura", "Data scientist", 4100),
  ("Ali", "Data engineer", 3200),
  ("Steve", "Developer", 3600)
  ).toDF("name", "role", "salary")

staffDF.show()

// COMMAND ----------

import spark.implicits.localSeqToDatasetHolder
val staffSeq = Seq(
  ("John", "Data scientist", 4500),
  ("James", "Data engineer", 3200),
  ("Laura", "Data scientist", 4100),
  ("Ali", "Data engineer", 3200),
  ("Steve", "Developer", 3600)
  )

val staffDF = localSeqToDatasetHolder(staffSeq).toDF("name", "role", "salary")
staffDF.show()

// COMMAND ----------

val staffDF = Seq(
  ("John", "Data scientist", 4500),  
  ("James", "Data engineer", 3200), 
  ("Laura", "Data scientist", 4100), 
  ("Ali", "Data engineer", 3200),  
  ("Steve", "Developer", 3600)  
  ).toDF()

staffDF.show()

// COMMAND ----------

val colList =List("name", "role", "salary")
val staffDF = Seq(
  ("John", "Data scientist", 4500),  
  ("James", "Data engineer", 3200), 
  ("Laura", "Data scientist", 4100), 
  ("Ali", "Data engineer", 3200),  
  ("Steve", "Developer", 3600)  
  ).toDF(colList : _*)
staffDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Creating a DataFrame using case classes

// COMMAND ----------

import org.apache.spark.sql.DataFrame

case class Employee(name: String, role: String, salary: Integer)
val staffWithCaseDF: DataFrame = spark.createDataFrame(Seq(Employee("John", "Data scientist", 4500),
                                                  Employee("James", "Data engineer", 3200),
                                                  Employee("Laura", "Data scientist", 4100),
                                                  Employee("Ali", "Data engineer", 3200),
                                                  Employee("Steve", "Developer", 3600)  
                                             ))
staffWithCaseDF.show()

// COMMAND ----------

val staffWithCaseDF = Seq(Employee("John", "Data scientist", 4500),
    Employee("James", "Data engineer", 3200),
    Employee("Laura", "Data scientist", 4100),
    Employee("Ali", "Data engineer", 3200),
    Employee("Steve", "Developer", 3600)).toDF()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### DataFrames vs Datasets

// COMMAND ----------

// This gives a compile-time error
import spark.implicits._
case class Employee(name: String, role: String, salary: Integer)
// val data = Seq(Employee("John", "Data scientist", "4500"),
//                Employee("James", "Data engineer", 3200),
//                Employee("Laura", "Data scientist", 4100),
//                Employee("Ali", "Data engineer", 3200),
//                Employee("Steve", "Developer", 3600))
//val staffDS = spark.createDataset(data)

// COMMAND ----------

// This gives a run-time error
import org.apache.spark.sql.types._
val someData = Seq(
  Row("John", "Data scientist", "4500"),
  Row("James", "Data engineer", 3200),
  Row("Laura", "Data scientist", 4100),
  Row("Ali", "Data engineer", 3200),
  Row("Steve", "Developer", 3600)
)

val someSchema = StructType(List(
  StructField("name", StringType, true),
  StructField("role", StringType, true),
  StructField("salary", IntegerType, true))
)

// val staffDF = spark.createDataFrame(
//   spark.sparkContext.parallelize(someData),
//   someSchema
// )

// COMMAND ----------

// MAGIC %md
// MAGIC #### Dispalying DataFrames

// COMMAND ----------

staffDF.show()

// COMMAND ----------

staffDF.show(3)

// COMMAND ----------

display(staffDF)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Importing a dataframe

// COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/"))

// COMMAND ----------

val csvFile = "/databricks-datasets/learning-spark-v2/mnm_dataset.csv"

// COMMAND ----------

val df = spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .load(csvFile)

// COMMAND ----------

df.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Showing the schema

// COMMAND ----------

staffDF.printSchema()

// COMMAND ----------

println(staffDF.schema)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Shape of dataframe

// COMMAND ----------

println(staffDF.count)

// COMMAND ----------

staffDF.show(staffDF.count.toInt)

// COMMAND ----------

staffDF.columns

// COMMAND ----------

println(staffDF.columns.size)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Displaying the rows

// COMMAND ----------

staffDF.head(3) 

// COMMAND ----------

staffDF.take(3)

// COMMAND ----------

staffDF.first()

// COMMAND ----------

staffDF.limit(3).show() 

// COMMAND ----------

// MAGIC %md
// MAGIC #### Calculating the statistics

// COMMAND ----------

staffDF.describe().show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Null values

// COMMAND ----------

val staffWithNullDF = Seq(
  ("John", null, 4500),
  ("James", "Data engineer", 3200),
  ("Laura", null, 4100),
  (null, "Data engineer", 3200),
  ("Steve", "Developer", 3600)
  ).toDF("name", "role", "salary")

staffWithNullDF.show()

// COMMAND ----------

staffWithNullDF.printSchema()

// COMMAND ----------

// This won't run
val someRows = Seq(
  ("John", "Data scientist", null),
  ("James", "Data engineer", 3200),
  ("Laura", "Data scientist", 4100),
  ("Ali", "Data engineer", 3200),
  ("Steve", "Developer", 3600)
  )

//someRows.toDF("name", "role", "salary").show()

// COMMAND ----------

val someRows: Seq[(String, String, Integer)] = Seq(
  ("John", "Data scientist", null),
  ("James", "Data engineer", 3200),
  ("Laura", "Data scientist", 4100),
  ("Ali", "Data engineer", 3200),
  ("Steve", "Developer", 3600)
)
someRows.toDF("name", "role", "salary").show()

// COMMAND ----------

val someRows: Seq[(Option[String], Option[String], Option[Integer])] = Seq(
  (Some("John"), Some("Data scientist"), None),
  (Some("James"), Some("Data engineer"), Some(3200)),
  (Some("Laura"), Some("Data scientist"), Some(4100)),
  (Some("Ali"), Some("Data engineer"), Some(3200)),
  (Some("Steve"), Some("Developer"), Some(3600))
)

someRows.toDF("name", "role", "salary").show()

// COMMAND ----------

// This won't compile
// val someRows: Seq[(String, String, Int)] = Seq(
//   ("John", "Data scientist", null),
//   ("James", "Data engineer", 3200),
//   ("Laura", "Data scientist", 4100),
//   ("Ali", "Data engineer", 3200),
//   ("Steve", "Developer", 3600)
// )

//someRows.toDF("name", "role", "salary").show()

// COMMAND ----------

case class Employee(name: String, role: String, salary: Option[Integer])
val someRows = Seq(Employee("John", "Data scientist", Some(4500)),
    Employee("James", "Data engineer", None),
    Employee("Laura", "Data scientist", Some(4100)),
    Employee("Ali", "Data engineer", Some(3200)),
    Employee("Steve", "Developer", None))
someRows.toDF("name", "role", "salary").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Columns

// COMMAND ----------

import org.apache.spark.sql.{functions=>F}
val colName = F.col("c1")

// COMMAND ----------

val colName = F.column("c1")

// COMMAND ----------

val colName = $"c1" 

// COMMAND ----------

val number ="5"
val colName = $"column${number}"

// COMMAND ----------

val colName = 'c1

// COMMAND ----------

staffDF.columns

// COMMAND ----------

staffDF("name")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Selecting columns

// COMMAND ----------

staffDF.select("name", "role").show()

// COMMAND ----------

staffDF.select(F.col("name")).show()
staffDF.select(staffDF("name")).show()
staffDF.select($"name").show()

// COMMAND ----------

staffDF.select("name").show()

// COMMAND ----------

staffDF.select('name).show()

// COMMAND ----------

// This won't compile
//staffDF('name)
//staffDF($"name")

// COMMAND ----------

val colNames = List("name", "role", "salary")
staffDF.select(colNames.head, colNames.tail: _*).show()

// COMMAND ----------

staffDF.select(colNames.map(c => F.col(c)): _*).show()

// COMMAND ----------

staffDF.select(colNames.map(F.col): _*).show()

// COMMAND ----------

staffDF.select(F.col("*")).show()

// COMMAND ----------

staffDF.select(staffDF.columns.slice(0, 2).head, staffDF.columns.slice(0, 2).tail: _*).show()

// COMMAND ----------

staffDF.select("name").rdd.map(r => r(0)).collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Dropping duplicates

// COMMAND ----------

val staffWithDuplicatesDF = Seq(
  ("John", "Data scientist", 4500),
  ("James", "Data engineer", 3200),
  ("Laura", "Data scientist", 4100),
  ("Ali", "Data engineer", 3200),
  ("Steve", "Developer", 3600),
  ("John", "Data scientist", 4500)
  ).toDF("name", "role", "salary")

staffWithDuplicatesDF.dropDuplicates("role").show()

// COMMAND ----------

staffWithDuplicatesDF.dropDuplicates().show()

// COMMAND ----------

staffWithDuplicatesDF.select("role").distinct.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Column expressions

// COMMAND ----------

staffDF.select($"salary" * 1.2 + 100).show()

// COMMAND ----------

val colExpr = $"salary" * 1.2 + 100

// COMMAND ----------

staffDF.select(colExpr).show()

// COMMAND ----------

// This won't compile
//staffDF.select("name", $"salary" * 1.2 + 100).show()

// COMMAND ----------

staffDF.select($"name", $"salary" * 1.2 + 100).show()

// COMMAND ----------

staffDF.select($"salary" > 4000).show()

// COMMAND ----------

staffDF.select(F.concat($"name", F.lit("-"), $"role")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Column aliases

// COMMAND ----------

val colExpr  = ($"salary" * 1.2 + 100).name("bonus")

// COMMAND ----------

staffDF.select(colExpr).show()

// COMMAND ----------

import org.apache.spark.sql.types.Metadata
val colExpr  = ($"salary" * 1.2 + 100).as("bonus", Metadata.fromJson("""{"desc": "2021 bonus"}"""))
val newStaffDF = staffDF.select(colExpr)
newStaffDF.schema.foreach{c => println(s"${c.name}, ${c.metadata.toString}")}

// COMMAND ----------

staffDF.select(F.expr("salary * 1.2 + 100").alias("bonus")).show()

// COMMAND ----------

staffDF.select(F.expr("salary > 4000")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Math functions

// COMMAND ----------

val df = Seq(1, 10, 20, 30, 40).toDF("number")
df.select(F.round(F.log("number"), 3)).show()

// COMMAND ----------

val df = Seq(50.6892, 206.892, 1268).toDF("number")
df.select(F.round($"number", -2)).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Creating a new column

// COMMAND ----------

val newStaffDF = staffDF.withColumn("vacation", F.lit(15))
newStaffDF.show()

// COMMAND ----------

val newStaffDF = staffDF.withColumn("salary", $"salary" + 300)
newStaffDF.show()

// COMMAND ----------

val newStaffDF = staffDF.select(F.col("*"), ($"salary" * 1.2 + 100).name("bonus"))
newStaffDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Renaming Columns

// COMMAND ----------

val newStaffDF = staffDF.withColumnRenamed("name", "first name").withColumnRenamed("role", "job")
newStaffDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Changing data type of columns

// COMMAND ----------

import org.apache.spark.sql.types.DoubleType
val temp = Seq(("2020-01-01 07:30:00", "17.0"), 
  ("2020-01-02 07:30:00", "25.5"),  
  ("2020-01-03 07:30:00", "19.5"),  
  ("2020-01-04 07:30:00", "21.2"),  
  ("2020-01-05 07:30:00", "18.0"), 
  ("2020-01-06 07:30:00", "20.5")
  ).toDF("time", "temperature")

val temperatureDF = temp.withColumn("temperature", F.col("temperature").cast(DoubleType))
temperatureDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Canonical string representation

// COMMAND ----------

val temp = Seq(("2020-01-01 07:30:00", "17.0"), 
  ("2020-01-02 07:30:00", "25.5"),  
  ("2020-01-03 07:30:00", "19.5"),  
  ("2020-01-04 07:30:00", "21.2"),  
  ("2020-01-05 07:30:00", "18.0"), 
  ("2020-01-06 07:30:00", "20.5")
  ).toDF("time", "temperature")

val temperatureDF = temp.withColumn("temperature", F.col("temperature").cast("double"))
temperatureDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Timestamp

// COMMAND ----------

val temp = Seq(("2020-01-01 07:30:00", 17.0), 
  ("2020-01-02 07:30:00", 25.5),  
  ("2020-01-03 07:30:00", 19.5),  
  ("2020-01-04 07:30:00", 21.2),  
  ("2020-01-05 07:30:00", 18.0), 
  ("2020-01-06 07:30:00", 20.5)
  ).toDF("time", "temperature")

val temperatureDF = temp.withColumn("time", F.col("time").cast("timestamp"))
temperatureDF.show()

// COMMAND ----------

// MAGIC %md 
// MAGIC ##### Dropping a column

// COMMAND ----------

staffDF.drop("role", "salary").show()

// COMMAND ----------

val dropList = List("role", "salary")
staffDF.drop(dropList :_*).show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Rows

// COMMAND ----------

staffDF.map {row => 
            (row.getAs[String](0), row.getAs[Integer]("salary") * 1.2)
            }.toDF("name", "bonus").show()

// COMMAND ----------

staffDF.map {row => 
            (row.getString(0), row.getAs[Integer]("salary") * 1.2)
            }.toDF("name", "bonus").show()

// COMMAND ----------

staffDF.filter($"name" === "John").head().getAs[String]("name")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Splitting a column 

// COMMAND ----------

import spark.implicits._
val colList = List("name", "role", "salary")
val rawDF = Seq(
  ("John, Data scientist", 4500),
  ("James, Data engineer", 3200),
  ("Laura, Data scientist", 4100)).toDF()

val splitData = rawDF.map(f=>{val nameRole = f.getAs[String](0).split(",")
                                 val salary = f.getAs[Integer](1)
                                 (nameRole(0), nameRole(1), salary)
                                })

val splitDF = splitData.toDF(colList: _*)
splitDF.show()

// COMMAND ----------

import spark.implicits._
val rawDF = Seq(
  ("John, Data scientist", 4500),
  ("James, Data engineer", 3200),
  ("Laura, Data scientist", 4100)).toDF("name-role", "salary")
rawDF.select($"name-role", F.split($"name-role", ",").as("split_col")).show(false)

// COMMAND ----------

import spark.implicits._
val rawDF = Seq(
  ("John, Data scientist", 4500),
  ("James, Data engineer", 3200),
  ("Laura, Data scientist", 4100)).toDF("name-role", "salary")
val splitDF = rawDF.select(F.split($"name-role", ",").as("split_col"), $"salary")
splitDF.select(
  $"split_col".getItem(0).as("name"),
  $"split_col".getItem(1).as("role"),
  $"salary").show()

// COMMAND ----------

rawDF.select(F.col("*"), F.split($"name-role", ",").as("split_col")).select(
    (0 to 1).map(i => F.col("split_col").getItem(i).as(s"col${i+1}")) :+ F.col("*") : _*
).drop("name-role", "split_col").show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### UDFs

// COMMAND ----------

val numDF = Seq(6, 9, 12, 3, 10).toDF("number")
val isEven = (number: Int) => number % 2 == 0
val isEvenUDF = F.udf(isEven)
val newDF = numDF.withColumn("even", isEvenUDF($"number"))
newDF.show()

// COMMAND ----------

val newDF = numDF.select(F.col("*"), isEvenUDF($"number") as "even")

// COMMAND ----------

isEvenUDF

// COMMAND ----------

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.types.BooleanType
import scala.util.Try

val outputEncoder = Try(ExpressionEncoder[Boolean]()).toOption
val inputEncoders = Try(ExpressionEncoder[Int]()).toOption :: Nil
val isEvenUDF = SparkUserDefinedFunction((_: Int) % 2 == 0, BooleanType, inputEncoders, outputEncoder)

// COMMAND ----------

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.ScalaUDF

val isEvenUDF = ScalaUDF((_: Int) % 2 == 0, BooleanType, Literal(6) :: Nil, Option(ExpressionEncoder[Int]()) :: Nil)

// COMMAND ----------

isEvenUDF.eval()

// COMMAND ----------

def func(salary:Integer, role:String): String = 
    (salary, role) match {
      case (x, "Data scientist") if x >= 4000 => "Senior"
      case (x, "Data engineer") if x > 3800  => "Senior"
      case (x, _) if x >= 3500  => "Senior"
      case _          => "Junior"
    }

val levelUDF = F.udf(func _)

val newStaffDF: DataFrame = staffDF.withColumn("level",                                                            
          levelUDF(F.col("salary"), F.col("role")))
newStaffDF.show()

// COMMAND ----------

import org.apache.spark.sql.functions.lit
val numDF = Seq(6, 9, 12, 3, 10).toDF("number")
val addConst = (number: Int, const: Int) => number + const
val addConstUDF = F.udf(addConst)
val newDF = numDF.withColumn("new_number", addConstUDF($"number", lit(5)))
newDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### UDFs with non-Column parameters

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Broadcast variables

// COMMAND ----------

val v = Array(1, 2, 3)
val broadcastV = spark.sparkContext.broadcast(v)
broadcastV.value

// COMMAND ----------

import org.apache.spark.broadcast.Broadcast
val jobCodes = Map("Data scientist" -> 0, "Data engineer" -> 1, "Developer" -> 2)
val broadcastJobCodes = spark.sparkContext.broadcast(jobCodes)
def mapJobCodeUDF(m: Broadcast[Map[String, Int]]) = udf((role: String) => m.value.getOrElse(role, -1))

staffDF.select($"role", mapJobCodeUDF(broadcastJobCodes)($"role")
  .as("job_code")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Null values in UDFs

// COMMAND ----------

//This won't run
val someRow: Seq[Integer] = Seq(6, 9, 12, null, 10)
val numDF = someRow.toDF("number")

def makeDouble(num: Integer): Integer = 2 * num
val makeDoubleUDF = F.udf[Integer, Integer](makeDouble)

// numDF.withColumn("number doubled", makeDoubleUDF(F.col("number"))).show()

// COMMAND ----------

def makeDouble(num: Integer): Option[Integer] = num match {
  case null => None
  case x => Some(2*x)
}
val makeDoubleUDF = F.udf[Option[Integer], Integer](makeDouble)

numDF.withColumn("number doubled", makeDoubleUDF(F.col("number"))).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Filtering the rows

// COMMAND ----------

val filteredDF = staffDF.filter($"salary" === 3200)  
filteredDF.show()

// COMMAND ----------

val filteredDF = staffDF.filter($"salary" > 3200 && $"name" =!= "Ali")  
filteredDF.show()

// COMMAND ----------

val filteredDF = staffDF.filter(($"salary" gt 3200) and ($"name" notEqual "Ali"))

// COMMAND ----------

staffDF.select($"salary" === 3200).show()

// COMMAND ----------

staffDF("salary") == 3200

// COMMAND ----------

val df = Seq(
  ("a", "a"),
  ("b", "b"),
  (null, null)).toDF("c1", "c2")
df.filter($"c1" === $"c2").show()

// COMMAND ----------

df.filter($"c1" <=> $"c2").show()

// COMMAND ----------

val filteredDF = staffDF.filter($"salary" > 3200 && $"name" =!= "Ali")

// COMMAND ----------

val filteredDF = staffDF.filter("salary > 3200 and name != 'Ali'")

// COMMAND ----------

staffDF.select($"salary".between(3200,3600)).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Methods for missing values

// COMMAND ----------

val staffWithNullDF = Seq(
  ("John", null, 4500),
  ("James", "Data engineer", 3200),
  ("Laura", null, 4100),
  (null, "Data engineer", 3200),
  ("Steve", "Developer", 3600)
  ).toDF("name", "role", "salary")

staffWithNullDF.show()

// COMMAND ----------

staffWithNullDF.filter($"role".isNull).show()

// COMMAND ----------

staffWithNullDF.filter("role IS NULL").show()

// COMMAND ----------

staffWithNullDF.filter($"role".isNotNull).show()

// COMMAND ----------

staffWithNullDF.na

// COMMAND ----------

staffWithNullDF.na.fill("N/A").show()

// COMMAND ----------

staffWithNullDF.na.fill("N/A", List("role")).show()

// COMMAND ----------

staffWithNullDF.na.drop().show() 

// COMMAND ----------

val someRows = Seq(
  (null, null, 4500),
  ("James", null, 3200),
  ("Laura", "Data scientist", 4100),
  ("Ali", "Data engineer", 3200),
  (null, "Developer", 3600)
)

someRows.toDF("name", "role", "salary").na.drop("all", List("name", "role")).show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### when() and otherwise()

// COMMAND ----------

staffDF.select(F.when(staffDF("role") === "Data scientist", 3)
              .alias("role code"))
              .show()

// COMMAND ----------

staffDF.select(F.when(staffDF("role") === "Data scientist", 3)
              .when(staffDF("role") === "Data engineer", 2)
              .otherwise(1).alias("role code"))
              .show()

// COMMAND ----------

staffDF.withColumn("salary", F.when($"role" === "Data scientist", $"salary" + 50).otherwise($"salary")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Partitions

// COMMAND ----------

val x  = (1 to 12).toList
val numberDF = x.toDF("number")
numberDF.rdd.getNumPartitions

// COMMAND ----------

numberDF.rdd.getNumPartitions

// COMMAND ----------

numberDF.rdd.partitions.size

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
val rows = numberDF.rdd.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => Row.fromSeq(index +: x.toSeq)).iterator }.collect()
val someSchema = StructField("partitionIndex", IntegerType, true) +: numberDF.schema.fields
val numberDFWithPartIndex = spark.createDataFrame(spark.sparkContext.parallelize(rows), StructType(someSchema))
numberDFWithPartIndex.show()

// COMMAND ----------

val repNumberDF = numberDF.repartition(3)
repNumberDF.rdd.partitions.size

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
val rows = repNumberDF.rdd.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => Row.fromSeq(index +: x.toSeq)).iterator }.collect()
val someSchema = StructField("partitionIndex", IntegerType, true) +: repNumberDF.schema.fields
val repNumberDFWithPartIndex = spark.createDataFrame(spark.sparkContext.parallelize(rows), StructType(someSchema))
repNumberDFWithPartIndex.show()

// COMMAND ----------

val coalNumberDF = numberDF.coalesce(3)
val rows = coalNumberDF.rdd.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(x => Row.fromSeq(index +: x.toSeq)).iterator }.collect()
val someSchema = StructField("partitionIndex", IntegerType, true) +: coalNumberDF.schema.fields
val coalNumberDFWithPartIndex = spark.createDataFrame(spark.sparkContext.parallelize(rows), StructType(someSchema))
coalNumberDFWithPartIndex.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Adding an index

// COMMAND ----------

val numberWithID = repNumberDF.withColumn("id", F.monotonically_increasing_id())
numberWithID.show()

// COMMAND ----------

val repOneNumberDF = numberDF.coalesce(1)
repOneNumberDF.withColumn("id", F.monotonically_increasing_id()).show()

// COMMAND ----------

val zippedRDD = repNumberDF.rdd.zipWithIndex()
zippedRDD.collect()

// COMMAND ----------

import org.apache.spark.sql.types._
val rows= zippedRDD.map(r => Row.fromSeq(r._1.toSeq ++ Seq(r._2)))
val someSchema =  repNumberDF.schema.fields ++ Array(StructField("id", LongType, true))  
val dfWithID = spark.createDataFrame(rows, StructType(someSchema))
dfWithID.show()

// COMMAND ----------

val rows= zippedRDD.map{case (row, index) => Row.fromSeq(index +: row.toSeq)}
val someSchema = StructField("id", LongType, true) +: repNumberDF.schema.fields   
val dfWithID = spark.createDataFrame(rows, StructType(someSchema))
dfWithID.show()

// COMMAND ----------

dfWithID.filter($"id".between(1, 3)).show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Saving a Dataset

// COMMAND ----------

val path = "/tmp/staff_df.csv"
staffDF.write.mode("overwrite").option("header", "true").format("csv").save(path)

// COMMAND ----------

display(dbutils.fs.ls("/tmp/staff_df.csv"))

// COMMAND ----------

// MAGIC %fs 
// MAGIC rm -r tmp

// COMMAND ----------

val path = "/tmp/staff_df"
val coalesced = staffDF.coalesce(1)
coalesced.write.mode("overwrite").option("header", "true").format("csv").save(path)

// COMMAND ----------

display(dbutils.fs.ls("/tmp/staff_df"))

// COMMAND ----------

val path = "/tmp/staff_df"
staffDF.write.mode("overwrite").option("header", "true").csv(path)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Sorting

// COMMAND ----------

staffDF.sort($"name").show()

// COMMAND ----------

staffDF.sort($"name".desc).show()

// COMMAND ----------

staffDF.sort(F.desc("name")).show()

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Aggregation

// COMMAND ----------

staffDF.groupBy("role").count().show()

// COMMAND ----------

staffDF.groupBy("role", "salary").count().show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Aggregate functions

// COMMAND ----------

staffDF.groupBy("role").agg(F.count("salary"), F.avg("salary")).show()

// COMMAND ----------

staffDF.agg(F.sum("salary")).show()

// COMMAND ----------

staffDF.select(F.sum("salary")).show()

// COMMAND ----------

// MAGIC %md 
// MAGIC ##### Pivoting

// COMMAND ----------

import spark.implicits._
val products = Seq(("P1", 100, "Vancouver"), ("P2", 150, "Vancouver"), ("P3", 130, "Vancouver"), 
  ("P4", 190, "Vancouver"), ("P1", 50, "Toronto"), 
  ("P2", 60, "Toronto"), ("P3", 70, "Toronto"), ("P4", 60, "Toronto"), 
  ("P1", 30, "Calgary"), ("P2", 140 ,"Calgary"), ("P3", 110, "Montreal"))
 
val productDF = products.toDF("productID", "quantity", "city")
productDF.show()

// COMMAND ----------

val pivotDF = productDF.groupBy("productID").pivot("city").sum("quantity")
pivotDF.show()

// COMMAND ----------

val cities = Seq("Calgary", "Montreal", "Toronto", "Vancouver")
val pivotDF =productDF.groupBy("productID").pivot("city", cities)
.sum("quantity")
pivotDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Unpivoting

// COMMAND ----------

val unPivotDF = pivotDF.select($"productID", F.expr("stack(3, 'Vancouver', Vancouver, 'Toronto', Toronto, 'Calgary', Calgary) as (city, quantity)"))
  .filter($"quantity".isNotNull)
unPivotDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Transpose

// COMMAND ----------

val df = Seq(("c1", 1), ("c2", 2), ("c3", 3), ("c4", 4)).toDF("col1", "col2")
df.show()

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val schema = StructType(df.select(F.collect_list("col1")).first().getAs[Seq[String]](0).map(x => StructField(x, IntegerType)))
val rows = Seq(Row.fromSeq(df.select(F.collect_list("col2")).first().getAs[Seq[Integer]](0)))
val data = spark.sparkContext.parallelize(rows)

spark.createDataFrame(data, schema).show()

// COMMAND ----------

val df1 = Seq(("c1", 1, "a"), ("c2", 2, "b"), ("c3", 3, "c"), ("c4", 4, "d")).toDF("col1", "col2", "col3")
df1.show()

// COMMAND ----------

val schema = StructType(df1.select(F.collect_list("col1")).first().getAs[Seq[String]](0).map(x => StructField(x, StringType)))
val rows = Seq(Row.fromSeq(df1.select(F.collect_list("col2")).first().getAs[Seq[Integer]](0).map(_.toString)),
                                                Row.fromSeq(df1.select(F.collect_list("col3")).first().getAs[Seq[String]](0)))
val data = spark.sparkContext.parallelize(rows)

spark.createDataFrame(data, schema).show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Window functions

// COMMAND ----------

temperatureDF.show()

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import spark.implicits._
import org.apache.spark.sql.{functions=>F}

val w = Window.orderBy("time").rowsBetween(-2, Window.currentRow)  
temperatureDF.withColumn("rolling_average", F.round(F.avg("temperature").over(w), 2)).show()

// COMMAND ----------

val w = Window.orderBy($"time".desc).rowsBetween(-2, Window.currentRow)  
temperatureDF.withColumn("rolling_average", F.round(F.avg("temperature").over(w), 2)).show()

// COMMAND ----------

val w = Window.orderBy("time")
temperatureDF.withColumn("cum_sum", F.sum("temperature").over(w)).show()

// COMMAND ----------

val w = org.apache.spark.sql.expressions.Window.orderBy("time")  
temperatureDF.withColumn("lag_col", F.lag("temperature", 1).over(w)).show()

// COMMAND ----------

temperatureDF.withColumn("lead_col", F.lag("temperature", -1, 0).over(w)).show()

// COMMAND ----------

val windowSpec  = Window.partitionBy("role").orderBy("salary")
staffDF.withColumn("row_number", F.row_number.over(windowSpec)).show()

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import spark.implicits._

val df = Seq((1, 1),(2, 2), (2, 3), (2, 4), (4, 5), (6, 6), (7, 7)).toDF("id", "num")
val w = Window.orderBy("id").rowsBetween(Window.currentRow, 1)  
df.withColumn("rolling_sum", F.sum("num").over(w)).show()

// COMMAND ----------

val w1 = Window.orderBy("id").rangeBetween(Window.currentRow, 1)  
df.withColumn("rolling_sum", F.sum("num").over(w1)).show()

// COMMAND ----------

val w2 = Window.orderBy($"id" * 2).rangeBetween(Window.currentRow, 1)  
df.withColumn("rolling_sum", F.sum("num").over(w1)).withColumn("id*2", $"id"*2).show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Joins

// COMMAND ----------

staffDF.show()

// COMMAND ----------

val ageDF = Seq(
  ("John", 45),
  ("James", 25),
  ("Laura", 30),
  ("Will", 28)
  ).toDF("name", "age")
ageDF.show()


// COMMAND ----------

// MAGIC %md
// MAGIC ##### Inner join

// COMMAND ----------

val joinedDF = staffDF.join(ageDF, staffDF("name") === ageDF("name"), "inner")
joinedDF.show()

// COMMAND ----------

val joinedDF = staffDF.as("df1").join(ageDF.as("df2"), $"df1.name" === $"df2.name", "inner")
joinedDF.show()

// COMMAND ----------

val joinedDF = staffDF.as("df1").join(staffDF.as("df2"), $"df1.salary" < $"df2.salary", "inner")
joinedDF.show()

// COMMAND ----------

val ageDF1 = Seq(
  ("John", "Data scientist", 45),
  ("James", "Data engineer", 25),
  ("Laura", "Data scientist", 30),
  ("Will", "Data engineer", 28)
  ).toDF("employee_name", "employee_role", "age")
ageDF1.show()

// COMMAND ----------

val joinedDF = staffDF.join(ageDF1, $"name" === $"employee_name" && $"role" === $"employee_role", "inner")
joinedDF.show()

// COMMAND ----------

staffDF.join(ageDF, Seq("name"), "inner").show()

// COMMAND ----------

staffDF.join(ageDF, "name").show()

// COMMAND ----------

val joinedDF = staffDF.join(ageDF).where(staffDF("name") === ageDF("name"))
joinedDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Left outer and right outer joins

// COMMAND ----------

val joinedDF = staffDF.join(ageDF, staffDF("name") === ageDF("name"), "left")
joinedDF.show()

// COMMAND ----------

val joinedDF = staffDF.join(ageDF, Seq("name"), "left")
joinedDF.show()

// COMMAND ----------

//This won't compile
//val joinedDF = staffDF.join(ageDF, "name", "left")

// COMMAND ----------

val joinedDF = staffDF.join(ageDF, Seq("name"), "right")
joinedDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Full outer join

// COMMAND ----------

staffDF.join(ageDF, Seq("name"), "full").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Left semi join

// COMMAND ----------

val joinedDF = staffDF.join(ageDF, staffDF("name") === ageDF("name"), "leftsemi")
joinedDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Left anti join

// COMMAND ----------

val joinedDF = staffDF.join(ageDF, staffDF("name") === ageDF("name"), "leftanti")
joinedDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Cross join

// COMMAND ----------

val joinedDF = staffDF.join(ageDF, staffDF("name") === ageDF("name"), "cross")
joinedDF.show()

// COMMAND ----------

val joinedDF = staffDF.crossJoin(ageDF)
joinedDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Type-preserving Joins

// COMMAND ----------

case class someRow1(name: String, role: String)
case class someRow2(name: String, id: Integer)
val ds1 = Seq(someRow1("John", "Data scientist"), someRow1("James", "Data engineer")).toDS()
val ds2 = Seq(someRow2("John", 127), someRow2("Steve", 192)).toDS()

val joinedDS = ds1.joinWith(ds2, ds1("name") === ds2("name"), "left")
joinedDS.show(false)

// COMMAND ----------

joinedDS.head()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Concatenating Datasets and DataFrames

// COMMAND ----------

val df1 = Seq((1, 2, "a")).toDF("col0", "col1", "col2")
val df2 = Seq((4, 5, "b")).toDF("col0", "col1", "col2")
df1.union(df2).show

// COMMAND ----------

// This own't compile
val df1 = Seq((1, 2, "a")).toDF("col0", "col1", "col2")
val df2 = Seq((4, 5)).toDF("col0", "col1")
// df1.union(df2).show

// COMMAND ----------

val df1 = Seq((1, 2, "c")).toDF("col0", "col1", "col2")
val df2 = Seq(("d", 4, 5)).toDF("col2", "col0", "col1")
df1.union(df2).show

// COMMAND ----------

val df1 = Seq((1, 2, "c")).toDF("col0", "col1", "col2")
val df2 = Seq(("d", 4, 5)).toDF("col2", "col0", "col1")
df1.unionByName(df2).show

// COMMAND ----------

// This won't compile
case class someRow1(col1: String, col2:Integer, col3: Integer)
case class someRow2(col2:Integer, col3: Integer, col1: String)
val ds1 = Seq(someRow1("a", 2, 3)).toDS()
val ds2 = Seq(someRow2(5, 6, "b")).toDS()
// ds1.union(ds2).show

// COMMAND ----------

import org.apache.spark.sql.{functions=>F}
val df1 = Seq((1, 2), (3, 4)).toDF("col1", "col2")
val df2 = Seq(("a", "b"), ("c", "d")).toDF("col3", "col4")

df1.withColumn("row_id", F.monotonically_increasing_id())
 .join(df2.withColumn("row_id", F.monotonically_increasing_id()), "row_id")
 .drop("row_id")
 .show()

// COMMAND ----------


