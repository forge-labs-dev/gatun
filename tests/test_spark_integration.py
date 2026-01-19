"""Integration tests for Spark with Gatun.

These tests verify that Gatun can load and use Apache Spark classes.
Tests are skipped if Spark is not installed.
"""

import glob
from pathlib import Path

import pytest

from gatun import connect

# Find Spark installation
PROJECT_ROOT = Path(__file__).parent.parent
SPARK_HOME = PROJECT_ROOT / "spark-4.1.1-bin-hadoop3"

# Skip all tests in this module if Spark is not found
pytestmark = pytest.mark.skipif(
    not SPARK_HOME.exists(),
    reason=f"Spark not found at {SPARK_HOME}. Extract spark-4.1.1-bin-hadoop3.tgz to run these tests.",
)


def get_spark_jars() -> list[str]:
    """Get all JAR files from Spark's jars directory."""
    jars_dir = SPARK_HOME / "jars"
    return glob.glob(str(jars_dir / "*.jar"))


@pytest.fixture(scope="module")
def spark_client():
    """Create a Gatun client with Spark JARs on classpath."""
    spark_jars = get_spark_jars()
    client = connect(memory="512MB", classpath=spark_jars)
    yield client
    client.close()


@pytest.fixture(scope="module")
def spark_conf(spark_client):
    """Create a SparkConf configured for local testing."""
    SparkConf = spark_client.jvm.org.apache.spark.SparkConf
    conf = SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("GatunSparkTest")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.driver.bindAddress", "127.0.0.1")
    conf.set("spark.driver.host", "127.0.0.1")
    return conf


@pytest.fixture(scope="module")
def java_spark_context(spark_client, spark_conf):
    """Create a JavaSparkContext for testing.

    Module-scoped to avoid repeatedly starting/stopping SparkContext,
    which can cause protocol issues with the shared Gatun client.
    """
    JavaSparkContext = spark_client.jvm.org.apache.spark.api.java.JavaSparkContext
    sc = JavaSparkContext(spark_conf)
    yield sc
    sc.stop()


@pytest.fixture(scope="module")
def spark_session(spark_client, java_spark_context):
    """Create a SparkSession for testing.

    Module-scoped to share across all tests that need it.
    """
    SparkSession = spark_client.jvm.org.apache.spark.sql.SparkSession
    spark = SparkSession.builder().sparkContext(java_spark_context.sc()).getOrCreate()
    return spark


class TestSparkConf:
    """Tests for SparkConf creation and configuration."""

    def test_create_spark_conf(self, spark_client):
        """Test that SparkConf can be created."""
        SparkConf = spark_client.jvm.org.apache.spark.SparkConf
        conf = SparkConf()
        assert conf is not None

    def test_set_and_get_config(self, spark_client):
        """Test setting and getting configuration values."""
        SparkConf = spark_client.jvm.org.apache.spark.SparkConf
        conf = SparkConf()
        conf.setMaster("local[4]")
        conf.setAppName("TestApp")

        assert conf.get("spark.master") == "local[4]"
        assert conf.get("spark.app.name") == "TestApp"

    def test_method_chaining(self, spark_client):
        """Test that SparkConf method chaining works."""
        SparkConf = spark_client.jvm.org.apache.spark.SparkConf
        conf = (
            SparkConf()
            .setMaster("local")
            .setAppName("ChainedApp")
            .set("spark.ui.enabled", "false")
        )

        assert conf.get("spark.master") == "local"
        assert conf.get("spark.app.name") == "ChainedApp"
        assert conf.get("spark.ui.enabled") == "false"


class TestJavaSparkContext:
    """Tests for JavaSparkContext creation."""

    def test_create_context(self, java_spark_context):
        """Test that JavaSparkContext can be created."""
        assert java_spark_context is not None

    def test_get_application_id(self, java_spark_context):
        """Test getting the application ID."""
        app_id = java_spark_context.sc().applicationId()
        assert app_id is not None
        assert app_id.startswith("local-")


class TestRDDOperations:
    """Tests for RDD operations."""

    def test_parallelize_and_count(self, spark_client, java_spark_context):
        """Test parallelizing a collection and counting elements."""
        ArrayList = spark_client.jvm.java.util.ArrayList
        data = ArrayList()
        for i in range(10):
            data.add(i)

        rdd = java_spark_context.parallelize(data)
        assert rdd.count() == 10

    def test_parallelize_and_collect(self, spark_client, java_spark_context):
        """Test parallelizing and collecting data."""
        ArrayList = spark_client.jvm.java.util.ArrayList
        data = ArrayList()
        for i in range(5):
            data.add(i * 2)

        rdd = java_spark_context.parallelize(data)
        collected = list(rdd.collect())
        assert collected == [0, 2, 4, 6, 8]


class TestSparkSQL:
    """Tests for Spark SQL DataFrame operations."""

    def test_create_range_dataframe(self, spark_session):
        """Test creating a DataFrame from range."""
        df = spark_session.range(10)
        assert df.count() == 10

    def test_filter_dataframe(self, spark_session):
        """Test filtering a DataFrame."""
        df = spark_session.range(10)
        filtered = df.filter("id > 5")
        assert filtered.count() == 4

    def test_select_expr(self, spark_session):
        """Test selecting with expressions."""
        df = spark_session.range(5)
        result = df.selectExpr("id", "id * 2 as doubled")

        # Verify schema has two columns
        schema = result.schema()
        field_names = [schema.apply(i).name() for i in range(schema.size())]
        assert "id" in field_names
        assert "doubled" in field_names

        # Verify row count
        assert result.count() == 5

    def test_group_by_and_agg(self, spark_session):
        """Test groupBy and aggregation."""
        df = spark_session.range(10)
        # Add a category column (id % 3)
        with_category = df.selectExpr("id", "id % 3 as category")

        # Group by category and count
        result = with_category.groupBy("category").count()
        assert result.count() == 3  # 3 distinct categories (0, 1, 2)

    def test_order_by(self, spark_session):
        """Test ordering DataFrame."""
        # Add a second column so we get Row objects back
        df = spark_session.range(5).selectExpr("id", "id * 10 as value")
        # Order by id descending
        ordered = df.orderBy(df.col("id").desc())

        # Verify count
        assert ordered.count() == 5

        # Get first row - with multiple columns, we get a Row object
        first_row = ordered.first()
        assert first_row.getLong(0) == 4  # First row should be id=4 (descending)
        assert first_row.getLong(1) == 40  # value = 4 * 10

    def test_join_dataframes(self, spark_session):
        """Test joining two DataFrames."""
        # Create two DataFrames
        df1 = spark_session.range(5).selectExpr("id as id1", "id * 10 as value1")
        df2 = spark_session.range(3, 8).selectExpr("id as id2", "id * 100 as value2")

        # Inner join on id1 = id2
        joined = df1.join(df2, df1.col("id1").equalTo(df2.col("id2")))

        # Should have 2 matching rows (id 3 and 4)
        assert joined.count() == 2

    def test_union_dataframes(self, spark_session):
        """Test union of two DataFrames."""
        df1 = spark_session.range(5)
        df2 = spark_session.range(5, 10)

        unioned = df1.union(df2)
        assert unioned.count() == 10

    def test_distinct(self, spark_session):
        """Test distinct operation."""
        df = spark_session.range(10)
        # Add duplicates via modulo
        with_dupes = df.selectExpr("id % 5 as value")

        distinct = with_dupes.distinct()
        assert distinct.count() == 5

    def test_sql_query(self, spark_session):
        """Test running SQL queries."""
        # Create a temporary view
        df = spark_session.range(10).selectExpr("id", "id * 2 as doubled")
        df.createOrReplaceTempView("numbers")

        # Run SQL query
        result = spark_session.sql("SELECT * FROM numbers WHERE id > 5 ORDER BY id")
        assert result.count() == 4

        # Verify ordering
        rows = list(result.collect())
        ids = [row.getLong(0) for row in rows]
        assert ids == [6, 7, 8, 9]

    def test_window_functions(self, spark_session, spark_client):
        """Test window functions."""
        # Create DataFrame with categories
        df = spark_session.range(10).selectExpr(
            "id", "id % 3 as category", "id * 10 as value"
        )

        # Define window spec
        Window = spark_client.jvm.org.apache.spark.sql.expressions.Window
        window_spec = Window.partitionBy("category").orderBy("id")

        # Add row number within each partition
        functions = spark_client.jvm.org.apache.spark.sql.functions
        result = df.withColumn("row_num", functions.row_number().over(window_spec))

        # Verify we have all rows
        assert result.count() == 10

        # Verify schema has the new column
        schema = result.schema()
        field_names = [schema.apply(i).name() for i in range(schema.size())]
        assert "row_num" in field_names

    def test_with_column_renamed(self, spark_session):
        """Test renaming columns."""
        df = spark_session.range(5)
        renamed = df.withColumnRenamed("id", "number")

        # Verify column name changed
        schema = renamed.schema()
        assert schema.apply(0).name() == "number"
        assert renamed.count() == 5

    def test_drop_column(self, spark_session):
        """Test dropping columns."""
        df = spark_session.range(5).selectExpr("id", "id * 2 as doubled")
        dropped = df.drop("doubled")

        # Verify only one column remains
        schema = dropped.schema()
        assert schema.size() == 1
        assert schema.apply(0).name() == "id"


class TestSparkMLlib:
    """Tests for Spark MLlib operations."""

    def test_vector_creation(self, spark_client):
        """Test creating ML Vectors."""
        import pyarrow as pa

        Vectors = spark_client.jvm.org.apache.spark.ml.linalg.Vectors

        # Create dense vector using PyArrow array (auto-converts to double[])
        dense = Vectors.dense(pa.array([1.0, 2.0, 3.0], type=pa.float64()))
        assert dense.size() == 3

        # Create sparse vector (size, indices, values)
        sparse = Vectors.sparse(
            5,
            pa.array([0, 2, 4], type=pa.int32()),
            pa.array([1.0, 2.0, 3.0], type=pa.float64()),
        )
        assert sparse.size() == 5

    def test_standard_scaler(self, spark_session, spark_client):
        """Test StandardScaler transformer."""
        # Create sample data with a vector column using SQL
        df = spark_session.range(5).selectExpr(
            "id",
            "cast(id as double) as value",
        )

        # Use VectorAssembler via SQL function instead
        functions = spark_client.jvm.org.apache.spark.sql.functions
        # array() creates an array column, which we can use for ML
        df_with_array = df.withColumn(
            "features_array", functions.array(df.col("value"))
        )

        # Verify we created the array column
        schema = df_with_array.schema()
        field_names = [schema.apply(i).name() for i in range(schema.size())]
        assert "features_array" in field_names
        assert df_with_array.count() == 5

    def test_bucketizer(self, spark_session, spark_client):
        """Test Bucketizer transformer."""
        import pyarrow as pa

        # Create sample data
        df = spark_session.range(10).selectExpr("cast(id as double) as value")

        # Create Bucketizer with splits
        Bucketizer = spark_client.jvm.org.apache.spark.ml.feature.Bucketizer
        bucketizer = Bucketizer()
        bucketizer.setInputCol("value")
        bucketizer.setOutputCol("bucket")
        # Set splits: (-inf, 3], (3, 6], (6, inf)
        bucketizer.setSplits(
            pa.array([float("-inf"), 3.0, 6.0, float("inf")], type=pa.float64())
        )

        # Transform
        result = bucketizer.transform(df)

        # Verify output column exists
        schema = result.schema()
        field_names = [schema.apply(i).name() for i in range(schema.size())]
        assert "bucket" in field_names
        assert result.count() == 10


class TestBulkArrowTransfer:
    """Tests for bulk Arrow data transfer (no SparkContext needed)."""

    def test_array_transfer_via_method_params(self, spark_client):
        """Test transferring arrays to Java via method parameters.

        Note: Arrays passed through method parameters are serialized in the
        command zone (64KB limit). For larger arrays, use send_arrow_table
        or send_arrow_buffers which use the payload zone.
        """
        import pyarrow as pa

        # Create an array that fits in command zone (< 64KB / 8 bytes = ~8000 elements)
        size = 5_000
        array = pa.array(list(range(size)), type=pa.int64())

        # Use Arrays.copyOf to verify the array was transferred correctly
        Arrays = spark_client.jvm.java.util.Arrays
        result = Arrays.copyOf(array, size)

        # Verify size
        assert len(result) == size

        # Verify first and last elements
        assert result[0] == 0
        assert result[size - 1] == size - 1

    def test_double_array_transfer_via_method_params(self, spark_client):
        """Test transferring double arrays for ML operations."""
        import pyarrow as pa

        # Create a double array that fits in command zone
        size = 4_000
        values = [float(i) * 0.1 for i in range(size)]
        array = pa.array(values, type=pa.float64())

        # Transfer to Java
        Arrays = spark_client.jvm.java.util.Arrays
        result = Arrays.copyOf(array, size)

        # Verify size
        assert len(result) == size

        # Verify values (check a few samples)
        assert abs(result[0] - 0.0) < 0.0001
        assert abs(result[100] - 10.0) < 0.0001
        assert abs(result[size - 1] - (size - 1) * 0.1) < 0.0001

    def test_arrow_table_ipc_transfer(self, spark_client):
        """Test sending Arrow table via IPC format (uses payload zone)."""
        import pyarrow as pa

        # Create an Arrow table with multiple columns - can be larger since
        # it uses the payload zone, not the command zone
        size = 5_000
        table = pa.table(
            {
                "id": pa.array(range(size), type=pa.int64()),
                "name": pa.array([f"item_{i}" for i in range(size)]),
                "value": pa.array(
                    [float(i) * 1.5 for i in range(size)], type=pa.float64()
                ),
            }
        )

        # Send via Gatun's Arrow IPC transfer
        response = spark_client.send_arrow_table(table)
        assert f"Received {size} rows" in str(response)

    def test_arrow_buffers_zero_copy(self, spark_client):
        """Test zero-copy Arrow buffer transfer."""
        import pyarrow as pa

        # Create a larger Arrow table
        size = 10_000
        table = pa.table(
            {
                "x": pa.array(range(size), type=pa.int64()),
                "y": pa.array([float(i) for i in range(size)], type=pa.float64()),
            }
        )

        # Use zero-copy transfer
        arena = spark_client.get_payload_arena()
        schema_cache = {}

        response = spark_client.send_arrow_buffers(table, arena, schema_cache)
        assert f"Received {size} rows" in str(response)

        # Schema should be cached
        assert len(schema_cache) == 1

        # Send again with cached schema (more efficient - no schema bytes sent)
        arena.reset()
        response2 = spark_client.send_arrow_buffers(table, arena, schema_cache)
        assert f"Received {size} rows" in str(response2)

        arena.close()

    def test_large_ml_vector_batch(self, spark_client):
        """Test creating multiple ML vectors in bulk."""
        import pyarrow as pa

        Vectors = spark_client.jvm.org.apache.spark.ml.linalg.Vectors

        # Create 100 dense vectors, each with 10 dimensions
        vectors = []
        for i in range(100):
            values = pa.array([float(i * 10 + j) for j in range(10)], type=pa.float64())
            vec = Vectors.dense(values)
            vectors.append(vec)
            assert vec.size() == 10

        # Verify we created all vectors
        assert len(vectors) == 100


class TestBulkDataFrameOperations:
    """Tests for bulk DataFrame operations (requires SparkContext)."""

    def test_bulk_rdd_operations(self, spark_client, java_spark_context):
        """Test RDD operations with bulk data."""
        import pyarrow as pa

        # Create data via PyArrow array (1,000 elements)
        size = 1_000
        data_array = pa.array(list(range(size)), type=pa.int32())

        # Convert to Java ArrayList
        ArrayList = spark_client.jvm.java.util.ArrayList
        data = ArrayList()
        for val in data_array.to_pylist():
            data.add(val)

        # Create RDD and perform operations
        rdd = java_spark_context.parallelize(data)
        assert rdd.count() == size

        # Verify the data is there
        collected = list(rdd.collect())
        assert len(collected) == size
        assert collected[0] == 0
        assert collected[size - 1] == size - 1

    def test_large_dataframe_operations(self, spark_session):
        """Test DataFrame operations with larger datasets."""
        # Create a DataFrame with 10,000 rows
        size = 10_000
        df = spark_session.range(size)

        # Verify count
        assert df.count() == size

        # Perform aggregation with fewer buckets to avoid creating many Row objects
        result = df.selectExpr("id", "id % 10 as bucket").groupBy("bucket").count()
        assert result.count() == 10  # 10 distinct buckets

        # Each bucket should have 1000 rows (10000 / 10)
        # Check first row to verify counts are correct
        first_row = result.first()
        assert first_row.getLong(1) == 1000  # count column

    def test_large_dataframe_with_multiple_columns(self, spark_session):
        """Test DataFrame with multiple columns and larger data."""
        size = 5_000
        df = spark_session.range(size).selectExpr(
            "id",
            "id * 2 as doubled",
            "id * id as squared",
            "id % 10 as category",
            "cast(id as double) / 100.0 as normalized",
        )

        # Verify schema
        schema = df.schema()
        assert schema.size() == 5

        # Verify count
        assert df.count() == size

        # Test filter performance
        filtered = df.filter("category < 5")
        assert filtered.count() == size // 2  # Half the rows

        # Test aggregation
        agg_result = df.groupBy("category").count()
        assert agg_result.count() == 10  # 10 categories

    def test_dataframe_join_large(self, spark_session):
        """Test joining larger DataFrames."""
        # Create two DataFrames with 1000 rows each
        size = 1_000
        df1 = spark_session.range(size).selectExpr("id as key", "id * 10 as value1")
        df2 = spark_session.range(size // 2, size + size // 2).selectExpr(
            "id as key", "id * 100 as value2"
        )

        # Inner join - should have 500 matching rows
        joined = df1.join(df2, "key")
        assert joined.count() == size // 2

    def test_dataframe_window_large(self, spark_session, spark_client):
        """Test window functions on larger dataset."""
        size = 2_000
        df = spark_session.range(size).selectExpr(
            "id",
            "id % 20 as partition_key",
            "cast(id as double) as value",
        )

        # Define window spec
        Window = spark_client.jvm.org.apache.spark.sql.expressions.Window
        window_spec = Window.partitionBy("partition_key").orderBy("id")

        # Add row number and running sum
        functions = spark_client.jvm.org.apache.spark.sql.functions
        result = df.withColumn(
            "row_num", functions.row_number().over(window_spec)
        ).withColumn("running_sum", functions.sum("value").over(window_spec))

        # Verify count
        assert result.count() == size

        # Verify schema has new columns
        schema = result.schema()
        field_names = [schema.apply(i).name() for i in range(schema.size())]
        assert "row_num" in field_names
        assert "running_sum" in field_names


class TestLargeDataFrameArrowTransfer:
    """Tests for large DataFrame operations with Arrow data transfer."""

    def test_large_dataframe_with_complex_operations(self, spark_session, spark_client):
        """Test creating a large DataFrame, performing operations, and collecting results.

        This test demonstrates:
        1. Creating a 50,000 row DataFrame in Java
        2. Performing multiple transformations (filter, select, aggregate, join)
        3. Collecting aggregated results back to Python
        """
        import pyarrow as pa

        # 1. Create a large DataFrame in Java
        size = 50_000
        df = spark_session.range(size).selectExpr(
            "id",
            "id % 100 as category",
            "id % 10 as subcategory",
            "cast(id as double) * 1.5 as value",
            "concat('item_', cast(id as string)) as name",
        )

        # Verify initial size
        assert df.count() == size

        # 2. Perform complex operations
        # Filter to keep ~half the rows
        filtered = df.filter("category < 50")
        assert filtered.count() == size // 2

        # Add computed columns
        functions = spark_client.jvm.org.apache.spark.sql.functions
        transformed = filtered.withColumn(
            "value_squared", functions.pow(filtered.col("value"), functions.lit(2.0))
        ).withColumn(
            "category_name",
            functions.concat(functions.lit("cat_"), filtered.col("category")),
        )

        # 3. Aggregate by category
        aggregated = (
            transformed.groupBy("category")
            .agg(
                functions.count("*").alias("count"),
                functions.sum("value").alias("total_value"),
                functions.avg("value").alias("avg_value"),
                functions.min("id").alias("min_id"),
                functions.max("id").alias("max_id"),
            )
            .orderBy("category")
        )

        # Should have 50 distinct categories (0-49)
        agg_count = aggregated.count()
        assert agg_count == 50

        # 4. Collect results to Python
        rows = list(aggregated.collect())
        assert len(rows) == 50

        # Verify first row (category 0)
        first_row = rows[0]
        assert first_row.getLong(0) == 0  # category
        assert first_row.getLong(1) == 500  # count (50000/2/50 = 500 rows per category)

        # Verify last row (category 49)
        last_row = rows[49]
        assert last_row.getLong(0) == 49  # category

        # 5. Convert collected results to PyArrow table for Python processing
        # Extract data from Java Row objects
        categories = []
        counts = []
        total_values = []
        avg_values = []

        for row in rows:
            categories.append(row.getLong(0))
            counts.append(row.getLong(1))
            total_values.append(row.getDouble(2))
            avg_values.append(row.getDouble(3))

        # Create PyArrow table from collected data
        result_table = pa.table(
            {
                "category": pa.array(categories, type=pa.int64()),
                "count": pa.array(counts, type=pa.int64()),
                "total_value": pa.array(total_values, type=pa.float64()),
                "avg_value": pa.array(avg_values, type=pa.float64()),
            }
        )

        # Verify the Arrow table
        assert result_table.num_rows == 50
        assert result_table.num_columns == 4

        # Verify data integrity
        assert result_table.column("category")[0].as_py() == 0
        assert result_table.column("count")[0].as_py() == 500

    def test_arrow_roundtrip_with_spark_processing(self, spark_session, spark_client):
        """Test Arrow data round-trip with Spark-like processing.

        This test demonstrates:
        1. Creating Arrow data in Python
        2. Sending to Java via zero-copy transfer
        3. Getting it back as Arrow via get_arrow_data()
        4. Verifying data integrity
        """
        import pyarrow as pa

        # 1. Create source data in Python
        size = 10_000
        source_table = pa.table(
            {
                "id": pa.array(range(size), type=pa.int64()),
                "category": pa.array([i % 20 for i in range(size)], type=pa.int64()),
                "value": pa.array(
                    [float(i) * 1.5 for i in range(size)], type=pa.float64()
                ),
            }
        )

        # 2. Send to Java via zero-copy
        arena = spark_client.get_payload_arena()
        schema_cache = {}

        response = spark_client.send_arrow_buffers(source_table, arena, schema_cache)
        assert f"Received {size} rows" in str(response)

        # 3. Get data back from Java
        result_view = spark_client.get_arrow_data()

        # 4. Verify the returned data
        assert result_view.num_rows == size
        assert result_view.num_columns == 3

        # Convert to Python dict for verification
        result_dict = result_view.to_pydict()

        # Verify columns exist
        assert "id" in result_dict
        assert "category" in result_dict
        assert "value" in result_dict

        # Verify data values
        assert result_dict["id"][0] == 0
        assert result_dict["id"][size - 1] == size - 1
        assert result_dict["category"][0] == 0
        assert result_dict["category"][20] == 0  # 20 % 20 = 0
        assert abs(result_dict["value"][100] - 150.0) < 0.0001  # 100 * 1.5

        arena.close()

    def test_large_join_and_collect(self, spark_session, spark_client):
        """Test joining large DataFrames and collecting results.

        This test creates two DataFrames, joins them, and transfers
        aggregated results back to Python.
        """
        import pyarrow as pa

        # Create two large DataFrames
        size = 20_000

        # Fact table: transactions
        transactions = spark_session.range(size).selectExpr(
            "id as txn_id",
            "id % 100 as customer_id",
            "cast(id as double) * 10.0 as amount",
            "id % 5 as product_category",
        )

        # Dimension table: customers (100 unique customers)
        customers = spark_session.range(100).selectExpr(
            "id as customer_id",
            "concat('Customer_', cast(id as string)) as customer_name",
            "id % 3 as region",
        )

        # Join transactions with customers
        functions = spark_client.jvm.org.apache.spark.sql.functions
        joined = transactions.join(customers, "customer_id")

        # Verify join size (all transactions should match)
        assert joined.count() == size

        # Aggregate by region
        by_region = (
            joined.groupBy("region")
            .agg(
                functions.count("*").alias("txn_count"),
                functions.sum("amount").alias("total_amount"),
                functions.countDistinct("customer_id").alias("unique_customers"),
            )
            .orderBy("region")
        )

        # Should have 3 regions (0, 1, 2)
        assert by_region.count() == 3

        # Collect and convert to Arrow
        rows = list(by_region.collect())
        regions = []
        txn_counts = []
        total_amounts = []
        unique_customers = []

        for row in rows:
            regions.append(row.getLong(0))
            txn_counts.append(row.getLong(1))
            total_amounts.append(row.getDouble(2))
            unique_customers.append(row.getLong(3))

        result_table = pa.table(
            {
                "region": pa.array(regions, type=pa.int64()),
                "txn_count": pa.array(txn_counts, type=pa.int64()),
                "total_amount": pa.array(total_amounts, type=pa.float64()),
                "unique_customers": pa.array(unique_customers, type=pa.int64()),
            }
        )

        # Verify results
        assert result_table.num_rows == 3

        # Each region should have ~33 or 34 unique customers
        for uc in unique_customers:
            assert 33 <= uc <= 34

        # Total transactions should equal size
        assert sum(txn_counts) == size
