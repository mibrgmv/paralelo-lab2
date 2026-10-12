package com.example.retail;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class OnlineRetail {

    static final String INPUT_PATH = "/opt/spark-data/data.csv";
    static final String OUTPUT_DIR = "/opt/spark-data/out";

    static final StructType SCHEMA = new StructType()
            .add("InvoiceNo", DataTypes.StringType)
            .add("StockCode", DataTypes.StringType)
            .add("Description", DataTypes.StringType)
            .add("Quantity", DataTypes.IntegerType)
            .add("InvoiceDate", DataTypes.StringType)
            .add("UnitPrice", DataTypes.DoubleType)
            .add("CustomerID", DataTypes.IntegerType)
            .add("Country", DataTypes.StringType);

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("OnlineRetail").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> sales = loadSales(spark);

        sales.printSchema();
        sales.show(5);
        System.out.println("rows: " + sales.count());

        write(topProducts(sales), "top-products");
        write(customerStats(sales), "customer-stats");

        spark.stop();
    }

    static Dataset<Row> loadSales(SparkSession spark) {
        return loadSales(spark, INPUT_PATH);
    }

    // Читает CSV, оставляет только продажи (без возвратов и нулевых цен) и убирает полные дубли строк
    static Dataset<Row> loadSales(SparkSession spark, String path) {
        Dataset<Row> raw = spark.read()
                .option("header", true)
                .option("encoding", "ISO-8859-1")
                .option("mode", "FAILFAST")
                .schema(SCHEMA)
                .csv(path);

        return raw
                .filter("Quantity > 0 AND UnitPrice > 0")
                .dropDuplicates();
    }

    // Топ-5 товаров по общему количеству проданных единиц
    static Dataset<Row> topProducts(Dataset<Row> sales) {
        return sales
                .groupBy("StockCode")
                .agg(mode(col("Description")).alias("Description"),
                        sum("Quantity").alias("TotalQuantity"))
                .orderBy(desc("TotalQuantity"))
                .limit(5);
    }

    // По каждому клиенту: число заказов, общая сумма, средний чек
    static Dataset<Row> customerStats(Dataset<Row> sales) {
        return sales
                .filter(col("CustomerID").isNotNull())
                .groupBy("CustomerID")
                .agg(countDistinct("InvoiceNo").alias("TotalOrders"),
                        sum(col("Quantity").multiply(col("UnitPrice"))).alias("TotalSpent"))
                .withColumn("AvgCheck", round(col("TotalSpent").divide(col("TotalOrders")), 2))
                .withColumn("TotalSpent", round(col("TotalSpent"), 2))
                .orderBy(desc("TotalSpent"));
    }

    static void write(Dataset<Row> result, String name) {
        result.coalesce(1)
                .write()
                .mode("overwrite")
                .option("header", true)
                .csv(OUTPUT_DIR + "/" + name);
    }
}
