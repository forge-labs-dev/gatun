#!/usr/bin/env python3
"""Test script to verify Spark classes can be loaded and used via Gatun.

This script tests:
1. Loading Spark JARs via classpath
2. Creating SparkConf
3. Creating JavaSparkContext
4. Running RDD operations
5. Running Spark SQL DataFrame operations

Usage:
    JAVA_HOME=/opt/homebrew/opt/openjdk uv run python scripts/test_spark_gatun.py
"""

import glob
import os
import sys

# Add src to path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from gatun import connect


def get_spark_jars(spark_home: str) -> list[str]:
    """Get all JAR files from Spark's jars directory."""
    jars_dir = os.path.join(spark_home, "jars")
    return glob.glob(os.path.join(jars_dir, "*.jar"))


def main():
    # Determine Spark home
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    spark_home = os.path.join(project_root, "spark-4.1.1-bin-hadoop3")

    if not os.path.exists(spark_home):
        print(f"Error: Spark not found at {spark_home}")
        print("Please extract spark-4.1.1-bin-hadoop3.tgz first")
        sys.exit(1)

    spark_jars = get_spark_jars(spark_home)
    print(f"Found {len(spark_jars)} Spark JARs")

    # Launch Gatun with Spark JARs on classpath using the convenient connect() API
    print("\n=== Launching Gatun with Spark classpath ===")

    client = connect(
        memory="512MB",
        classpath=spark_jars,
        debug=True,  # Show Java output for debugging
    )
    # Note: You may see warnings about "getSubject is not supported" - these are
    # from Hadoop libraries not yet updated for Java 17+ and can be ignored.
    # The functionality still works correctly.

    try:
        # Test 1: Create SparkConf
        print("\n=== Test 1: Creating SparkConf ===")
        SparkConf = client.jvm.org.apache.spark.SparkConf
        conf = SparkConf()
        conf.setMaster("local[2]")
        conf.setAppName("GatunSparkTest")
        conf.set("spark.ui.enabled", "false")  # Disable UI for testing
        conf.set("spark.driver.bindAddress", "127.0.0.1")  # Fix binding issue
        conf.set("spark.driver.host", "127.0.0.1")
        print(f"SparkConf created: {conf}")

        # Get config values back
        master = conf.get("spark.master")
        app_name = conf.get("spark.app.name")
        print(f"  master: {master}")
        print(f"  app.name: {app_name}")

        # Test 2: Create JavaSparkContext
        print("\n=== Test 2: Creating JavaSparkContext ===")
        JavaSparkContext = client.jvm.org.apache.spark.api.java.JavaSparkContext
        sc = JavaSparkContext(conf)
        print(f"JavaSparkContext created: {sc}")

        # Get some context info
        app_id = sc.sc().applicationId()
        print(f"  applicationId: {app_id}")

        # Test 3: Create an RDD and run a simple operation
        print("\n=== Test 3: Running simple Spark workflow ===")

        # Create a list in Java
        ArrayList = client.jvm.java.util.ArrayList
        data = ArrayList()
        for i in range(10):
            data.add(i)

        # Parallelize to create an RDD
        rdd = sc.parallelize(data)
        print(f"Created RDD: {rdd}")

        # Get count
        count = rdd.count()
        print(f"  count: {count}")

        # Collect results
        collected = rdd.collect()
        print(f"  collected: {list(collected)}")

        # Test 4: Filter operation (using Spark SQL)
        print("\n=== Test 4: Spark SQL DataFrame ===")

        # Create SparkSession from SparkContext
        SparkSession = client.jvm.org.apache.spark.sql.SparkSession
        spark = SparkSession.builder().sparkContext(sc.sc()).getOrCreate()
        print(f"  SparkSession: {spark}")

        # Create a simple DataFrame from range
        df = spark.range(10)
        print("  DataFrame schema:")
        df.printSchema()

        # Simple operations
        row_count = df.count()
        print(f"  Row count: {row_count}")

        # Filter and collect
        filtered = df.filter("id > 5")
        filtered_count = filtered.count()
        print(f"  Filtered count (id > 5): {filtered_count}")

        # Select with expression
        with_doubled = df.selectExpr("id", "id * 2 as doubled")
        print("  With doubled column:")
        with_doubled.show()

        # Stop SparkContext
        print("\n=== Stopping SparkContext ===")
        sc.stop()
        print("SparkContext stopped")

        print("\n=== SUCCESS: All tests passed! ===")

    finally:
        # Close client (also stops the server)
        client.close()


if __name__ == "__main__":
    main()
