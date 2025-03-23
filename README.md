# Spark Delta Lake MinIO Integration

This project demonstrates how to use Apache Spark with Delta Lake to read and write data to MinIO (S3-compatible storage). The main application showcases integration between these technologies and provides examples of common data operations.

## Main Application Overview

The `Main.java` file contains the primary application logic and demonstrates the following features:

My java version:
```
(base) donthireddy@MacBookPro databricks-simple % java --version
openjdk 17.0.7 2023-04-18
OpenJDK Runtime Environment Temurin-17.0.7+7 (build 17.0.7+7)
OpenJDK 64-Bit Server VM Temurin-17.0.7+7 (build 17.0.7+7, mixed mode)
(base) donthireddy@MacBookPro databricks-simple % 

```

### 1. Spark Session Configuration

```java
SparkSession spark = SparkSession.builder()
    .appName("Simple Spark Example")
    .master("local[*]")
    // ... configuration settings ...
    .getOrCreate();
```

Key configurations include:
- Local Spark cluster setup (`local[*]`)
- Delta Lake integration
- MinIO (S3) connectivity settings
- Custom warehouse directory configuration

### 2. MinIO Configuration

The application is configured to work with MinIO using the following settings:
```
mkdir -p ~/minio/data
docker run -d \
   -p 9000:9000 \
   -p 9001:9001 \
   --name minio \
   -v ~/minio/data:/data \
   -e "MINIO_ROOT_USER=vijay" \
   -e "MINIO_ROOT_PASSWORD=donthireddy" \
   quay.io/minio/minio server /data --console-address ":9001"
```
- Endpoint: `http://localhost:9000`
- Path Style Access: Enabled
- SSL: Disabled
- Authentication: Using access key and secret key
- S3A filesystem implementation

### 3. Data Operations

The application demonstrates several data operations:

#### JSON Reading
```java
Dataset<Row> df1 = spark.read()
    .option("multiline", "true")
    .json("s3a://vj-bucket/sample.json");
```
- Reads multiline JSON data from MinIO
- Demonstrates schema inference
- Shows nested JSON field access

#### Delta Lake Writing
```java
df.write()
    .format("delta")
    .save("s3a://" + deltaTablePath + "/test-" + timestamp);
```
- Writes data in Delta format
- Uses timestamp-based partitioning
- Demonstrates Delta Lake integration with MinIO

### 4. Data Structure

The application creates a sample dataset with the following schema:
- `name` (String)
- `age` (Integer)
- `department` (String)

Example data:
```java
List<Row> data = Arrays.asList(
    RowFactory.create("John Doe", 30, "Engineering"),
    RowFactory.create("Jane Smith", 28, "Marketing"),
    RowFactory.create("Bob Johnson", 35, "Sales")
);
```

## Prerequisites

- Java 11 or higher
- Apache Spark 3.4.1
- Delta Lake 2.4.0
- Running MinIO instance
- Maven for dependency management

## Configuration Requirements

### MinIO Setup
```properties
Endpoint: http://localhost:9000
Access Key: your_access_key
Secret Key: your_secret_key
```

### Dependencies
- Apache Spark Core and SQL
- Delta Lake Core
- Hadoop AWS
- AWS Java SDK

## Running the Application

1. Ensure MinIO is running and accessible
2. Build the project:
```
# for spark run the following code before the spark-submit
export SPARK_LOCAL_IP="127.0.0.1"

# package the jar
cd /Users/donthireddy/code/mygit/databricks-simple
mvn clean package

# submit the jar file to spark (make sure to use the jar-with-dependencies)
cd ~/code/spark-3.5.5-bin-hadoop3/

#the jar files provided in the following command are available in the 'libs' folder of the project
spark-submit --jars /Users/donthireddy/code/mygit/databricks-simple/libs/delta-core_2.12-2.4.0.jar,/Users/donthireddy/code/mygit/databricks-simple/libs/delta-storage-2.4.0.jar,/Users/donthireddy/code/mygit/databricks-simple/libs/hadoop-aws-3.3.4.jar,/Users/donthireddy/code/mygit/databricks-simple/libs/aws-java-sdk-bundle-1.12.533.jar --class=com.niharsystems.Main /Users/donthireddy/code/mygit/databricks-simple/target/databricks-simple-1.0-SNAPSHOT-jar-with-dependencies.jar

```

3. Run the application:
```bash
java -jar target/spark-example-1.0-SNAPSHOT.jar
```

## Key Features

1. **Delta Lake Integration**
    - ACID transactions
    - Schema enforcement
    - Time travel capabilities
    - Optimized storage format

2. **MinIO Compatibility**
    - S3A filesystem support
    - Path style access
    - Configurable authentication
    - SSL support (optional)

3. **Data Processing**
    - JSON file reading
    - Schema inference
    - Nested field access
    - Delta format writing

## Best Practices Demonstrated

1. **Resource Management**
    - Proper SparkSession initialization
    - Resource cleanup with `spark.stop()`
    - Error handling

2. **Configuration**
    - Externalized configuration
    - Secure credential management
    - Flexible storage paths

3. **Data Handling**
    - Structured data processing
    - Type-safe schema definition
    - Efficient data writing

## Notes

- The application uses local mode for development purposes
- Production deployments should configure appropriate cluster settings
- Credentials should be managed securely in production
- Delta Lake provides additional features not demonstrated in this example

## Troubleshooting

Common issues and solutions:
1. MinIO Connection Issues
    - Verify MinIO is running
    - Check endpoint configuration
    - Validate credentials

2. Delta Lake Errors
    - Ensure proper version compatibility
    - Check storage permissions
    - Verify path configurations

## Future Enhancements

Potential improvements:
1. Add configuration file support
2. Implement error recovery
3. Add data validation
4. Include more Delta Lake features
5. Add monitoring and logging # spark-scala-minio-delta
# spark-java-minio-delta
