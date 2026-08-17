# PySpark Practice Notebook

This repository contains my hands-on PySpark learning exercises performed in Databricks. The notebook focuses on core DataFrame and SQL operations used in real-world big data processing.

## Project Files

- [pyspark.ipynb](pyspark.ipynb) - main notebook with step-by-step PySpark practice
- [Data/orders.csv](Data/orders.csv) - sample order dataset in CSV format
- [Data/order_json.json](Data/order_json.json) - sample order dataset in JSON format

## Learning Objective

The goal of this project is to practice and understand the fundamentals of PySpark, especially:

- creating and reading DataFrames from CSV, JSON, and Parquet files
- inspecting schemas and data structure
- selecting and transforming columns
- filtering and sorting records
- removing duplicates and handling data quality issues
- grouping and aggregating data
- joining multiple datasets
- combining tables using union
- replacing null values and defining schemas
- pivot/unpivot operations
- using SQL views and Spark SQL
- window functions like row_number, rank, dense_rank, lag, and lead
- partitioning, repartitioning, and coalesce for storage optimization
- date manipulation functions
- exploding arrays and maps
- user-defined functions (UDFs)

## Topics Covered

### 1. Creating DataFrames

I practiced loading datasets from different file formats using Spark:

```python
# CSV
spark.read.csv("/Volumes/development/data/files/orders/orders.csv", header=True)

# JSON
spark.read.json("/Volumes/development/data/files/orders/order_json.json", multiLine=True)

# Parquet
spark.read.parquet("/Volumes/development/data/files/orders/orders.parquet")
```

This helped understand how Spark reads structured data and how to inspect schema and dataset contents.

### 2. DataFrame Transformations

I practiced selecting specific columns and creating new ones:

```python
from pyspark.sql.functions import *

df.select("order_id", "customer_name", "email")
df.withColumn("id", col("order_id") + 10)
df.withColumnRenamed("customer_name", "name")
```

These examples show how to shape a DataFrame without changing the original data source.

### 3. Filtering Data

Filtering is one of the most important operations in Spark. I worked with:

```python
df.filter(df.status == "Done").show()
df.filter(df.status != "Done").show()
df.filter((df.payment_method == "Debit Card") & (df.status == "Completed")).show()
df.filter(df.email.startswith("a")).show()
```

This helped understand conditional logic and string matching in PySpark.

### 4. Distinct and Deduplication

I learned the difference between `distinct()` and `dropDuplicates()`:

- `distinct()` removes duplicate rows across the full row
- `dropDuplicates([subset])` removes duplicates based on selected columns only

```python
df.distinct().show()
df.dropDuplicates(["age"]).show()
```

### 5. Sorting and Ordering

I practiced sorting values using `sort()` and `orderBy()`:

```python
df.sort("customer_name")
df.orderBy("customer_name")
df.sort(df.order_id.desc())
```

This is useful for ranking and presenting data in a structured way.

### 6. Aggregations and Group By

Group by operations are essential for data analysis. I worked with `count`, `avg`, `sum`, `max`, and `min`:

```python
df.groupBy("state").count()
df.groupBy("state").agg({"salary": "avg"})
df.groupBy("state").agg({"salary": "sum"})
```

This is similar to SQL `GROUP BY`.

### 7. Joins in PySpark

I learned the main join types:

- inner
- left
- right
- outer
- left_semi
- left_anti

```python
emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "inner").show()
emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left").show()
```

Joins are used to combine data across related tables.

### 8. Union and Duplicate Handling

I used `union()` to combine rows from two DataFrames and then removed duplicates:

```python
df1.union(df2).show()
df1.union(df2).distinct().show()
```

### 9. Null Handling

I used `fillna()` and `na.fill()` to replace missing values:

```python
df.fillna("")
df.na.fill(0)
df.fillna("UNKNOWN")
df.na.fill("blank", ["quantity"])
```

This is important for cleaning data before analytics and reporting.

### 10. Schema Definition

I also practiced defining custom schemas using `StructType` and `StructField`:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
```

This helps control data types and enforce quality rules for DataFrames.

### 11. Pivot and Unpivot

I practiced transforming data from long format to wide format using pivot:

```python
df.groupBy("region").pivot("product").sum("qty").show()
```

For unpivot, I used SQL-style `stack()` to turn columns back into rows.

### 12. UDFs (User Defined Functions)

I learned how to create custom logic using UDFs:

```python
def age_grp(age):
    if age is None:
        return "UNKNOWN"
    elif age < 18:
        return "MINOR"
    elif age >= 18 and age < 40:
        return "ADULT"
    else:
        return "SENIOR"

age_grp_udf = udf(age_grp, StringType())
df.withColumn("age_cat", age_grp_udf(df.age)).show()
```

This is helpful for custom transformations when built-in functions are not enough.

### 13. SQL Views with Spark SQL

I used `createOrReplaceTempView()` to expose DataFrames as temporary SQL tables:

```python
df.createOrReplaceTempView("temp")
```

Then I queried the data with SQL:

```sql
SELECT * FROM temp
SELECT COUNT(DISTINCT order_id), MIN(order_date) FROM test
```

This connects DataFrame APIs with SQL-based analytics.

### 14. Window Functions

I learned advanced analytical functions such as:

- `row_number()`
- `rank()`
- `dense_rank()`
- `lag()`
- `lead()`

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import *

w = Window.partitionBy("dept").orderBy(col("salary").desc())
df.withColumn("rn", row_number().over(w)).show()
```

These functions are used for ranking and comparing rows within groups.

### 15. Partitioning and Repartitioning

I practiced writing data with partitioning and using `repartition()` and `coalesce()`:

```python
df.write.mode("overwrite").partitionBy("region").parquet("/Volumes/development/data/files/sales")
df.repartition(3).write.mode("overwrite").partitionBy("region").parquet("/Volumes/development/data/files/sales2")
```

This is important for optimizing large-scale data storage and parallel processing.

### 16. Date Functions in PySpark

I worked with date formatting and date arithmetic:

```python
from pyspark.sql.functions import *

df.select("id", "date", date_format("date", "yyyy/MM/dd")).show()
df.select("date", date_add("date", 6)).show()
df.select("date", add_months("date", 1)).show()
df.select(current_date(), year(current_date())).show()
```

These functions help in extracting, transforming, and comparing date values.

### 17. Explode and posexplode

I learned how to transform arrays and maps into rows using `explode()` and `posexplode()`:

```python
from pyspark.sql.functions import *

df.select("id", explode("fruits")).show()
df.select("id", posexplode("fruits")).show()
df.select("id", explode("scores")).show()
```

This is very useful when working with nested or semi-structured data.

## Key Takeaways

This practice notebook helped me build a solid foundation in PySpark, especially in:

- understanding DataFrames and Spark architecture
- reading and cleaning real data
- transforming data with SQL-like operations
- using Spark SQL and DataFrame APIs together
- preparing data for analysis and reporting
- applying advanced transformations for real-world ETL tasks

## Tools Used

- Databricks
- Apache Spark
- PySpark DataFrame API
- Spark SQL

## Conclusion

This repository reflects my learning journey in PySpark step by step, starting from basic DataFrame operations and moving toward more advanced transformations, SQL integration, and analytics functions.

It is a practical notebook for learning and revisiting core Spark concepts during data engineering and analytics work.
