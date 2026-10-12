package com.example.retail;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class OnlineRetailTest {

    static final String HEADER = "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country";
    static final String SALE = "536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,12/1/2010 8:26,2.55,17850,United Kingdom";

    static SparkSession spark;

    @TempDir
    Path dir;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("OnlineRetailTest")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        spark.stop();
    }

    @Test
    void keepsSchema() throws IOException {
        assertEquals(OnlineRetail.SCHEMA, load(SALE).schema());
    }

    @Test
    void parsesSale() throws IOException {
        Row r = single(load(SALE));
        assertEquals("536365", r.getAs("InvoiceNo"));
        assertEquals("85123A", r.getAs("StockCode"));
        assertEquals("WHITE HANGING HEART T-LIGHT HOLDER", r.getAs("Description"));
        assertEquals(6, (int) r.getAs("Quantity"));
        assertEquals("12/1/2010 8:26", r.getAs("InvoiceDate"));
        assertEquals(2.55, (double) r.getAs("UnitPrice"));
        assertEquals(17850, (int) r.getAs("CustomerID"));
        assertEquals("United Kingdom", r.getAs("Country"));
    }

    static Stream<Named<String>> notSales() {
        return Stream.of(
                Named.of("cancellation", "C536383,35004C,SET OF 3 COLOURED  FLYING DUCKS,-1,12/1/2010 9:49,4.65,15311,United Kingdom"),
                Named.of("zero quantity", "536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,0,12/1/2010 8:26,2.55,17850,United Kingdom"),
                Named.of("negative quantity", "536589,21777,,-10,12/1/2010 16:50,0,,United Kingdom"),
                Named.of("zero price", "536414,22139,,56,12/1/2010 11:52,0,,United Kingdom"),
                Named.of("negative price", "A563186,B,Adjust bad debt,1,8/12/2011 14:51,-11062.06,,United Kingdom"));
    }

    @ParameterizedTest
    @MethodSource("notSales")
    void dropsNotSales(String row) throws IOException {
        List<String> invoices = load(SALE, row).select("InvoiceNo").as(Encoders.STRING()).collectAsList();
        assertEquals(List.of("536365"), invoices);
    }

    @Test
    void dropsExactDuplicates() throws IOException {
        assertEquals(1, load(SALE, SALE, SALE).count());
    }

    @Test
    void keepsRowsDifferingInOneField() throws IOException {
        assertEquals(2, load(SALE, SALE.replace(",6,", ",7,")).count());
    }

    @Test
    void decodesLatin1() throws IOException {
        Row r = single(load("539492,gift_0001_40,Dotcomgiftshop Gift Voucher £40.00,1,12/20/2010 10:14,34.04,,United Kingdom"));
        assertEquals("Dotcomgiftshop Gift Voucher £40.00", r.getAs("Description"));
    }

    @Test
    void keepsSaleWithoutCustomer() throws IOException {
        Row r = single(load("536544,21773,DECORATIVE ROSE BATHROOM BOTTLE,1,12/1/2010 14:32,2.51,,United Kingdom"));
        assertNull(r.getAs("CustomerID"));
    }

    static Stream<Named<String>> malformed() {
        return Stream.of(
                Named.of("quantity not a number", "536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,six,12/1/2010 8:26,2.55,17850,United Kingdom"),
                Named.of("price not a number", "536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,12/1/2010 8:26,abc,17850,United Kingdom"),
                Named.of("missing field", "536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,12/1/2010 8:26,2.55,17850"));
    }

    @ParameterizedTest
    @MethodSource("malformed")
    void failsOnMalformedRow(String row) throws IOException {
        Dataset<Row> sales = load(SALE, row);
        assertThrows(Exception.class, sales::count);
    }

    @Test
    void fullDataset() {
        Path data = Path.of("data/data.csv");
        assumeTrue(Files.exists(data), "data/data.csv not found");

        Dataset<Row> sales = OnlineRetail.loadSales(spark, data.toString()).cache();
        assertEquals(524_878, sales.count());
        assertEquals(0, sales.filter("Quantity <= 0 OR UnitPrice <= 0").count());
        assertEquals(0, sales.filter("InvoiceNo LIKE 'C%'").count());
        assertEquals(sales.count(), sales.distinct().count());
        sales.unpersist();
    }

    @Test
    void topProductsSumsQuantityPerProduct() throws IOException {
        Row r = single(OnlineRetail.topProducts(load(
                line("536365", "85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 6, 2.55, "17850"),
                line("536366", "85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 4, 2.55, "13047"),
                line("536367", "85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 2, 2.55, ""))));
        assertEquals("85123A", r.getAs("StockCode"));
        assertEquals(12L, (long) r.getAs("TotalQuantity"));
    }

    @Test
    void topProductsReturnsFiveLargestDescending() throws IOException {
        Dataset<Row> top = OnlineRetail.topProducts(load(
                line("536365", "10001", "A", 1, 1.0, "17850"),
                line("536365", "10002", "B", 6, 1.0, "17850"),
                line("536365", "10003", "C", 3, 1.0, "17850"),
                line("536365", "10004", "D", 5, 1.0, "17850"),
                line("536365", "10005", "E", 2, 1.0, "17850"),
                line("536365", "10006", "F", 4, 1.0, "17850")));
        List<String> codes = top.select("StockCode").as(Encoders.STRING()).collectAsList();
        assertEquals(List.of("10002", "10004", "10006", "10003", "10005"), codes);
    }

    @Test
    void topProductsTakesMostFrequentDescription() throws IOException {
        Row r = single(OnlineRetail.topProducts(load(
                line("536365", "85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 1, 2.55, "17850"),
                line("536366", "85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 1, 2.55, "17850"),
                line("536367", "85123A", "CREAM HANGING HEART T-LIGHT HOLDER", 1, 2.55, "17850"))));
        assertEquals("WHITE HANGING HEART T-LIGHT HOLDER", r.getAs("Description"));
    }

    @Test
    void customerStatsCountsOrdersSpentAndAvgCheck() throws IOException {
        Row r = single(OnlineRetail.customerStats(load(
                line("536365", "10001", "A", 2, 1.5, "17850"),
                line("536365", "10002", "B", 1, 3.0, "17850"),
                line("536366", "10001", "A", 1, 4.0, "17850"))));
        assertEquals(17850, (int) r.getAs("CustomerID"));
        assertEquals(2L, (long) r.getAs("TotalOrders"));
        assertEquals(10.0, (double) r.getAs("TotalSpent"), 1e-9);
        assertEquals(5.0, (double) r.getAs("AvgCheck"), 1e-9);
    }

    @Test
    void customerStatsRoundsToCents() throws IOException {
        Row r = single(OnlineRetail.customerStats(load(
                line("536365", "10001", "A", 3, 0.1, "17850"),
                line("536366", "10001", "A", 3, 0.1, "17850"),
                line("536367", "10001", "A", 4, 0.1, "17850"))));
        assertEquals(1.0, (double) r.getAs("TotalSpent"), 1e-9);
        assertEquals(0.33, (double) r.getAs("AvgCheck"), 1e-9);
    }

    @Test
    void customerStatsSkipsRowsWithoutCustomer() throws IOException {
        Row r = single(OnlineRetail.customerStats(load(
                line("536365", "10001", "A", 1, 1.0, "17850"),
                line("536366", "10001", "A", 5, 1.0, ""))));
        assertEquals(17850, (int) r.getAs("CustomerID"));
        assertEquals(1.0, (double) r.getAs("TotalSpent"), 1e-9);
    }

    @Test
    void customerStatsOneRowPerCustomerSortedBySpent() throws IOException {
        Dataset<Row> stats = OnlineRetail.customerStats(load(
                line("536365", "10001", "A", 5, 1.0, "17850"),
                line("536366", "10001", "A", 20, 1.0, "13047"),
                line("536367", "10001", "A", 4, 1.0, "12583"),
                line("536368", "10001", "A", 6, 1.0, "12583")));
        List<Integer> customers = stats.select("CustomerID").as(Encoders.INT()).collectAsList();
        assertEquals(List.of(13047, 12583, 17850), customers);
    }

    Dataset<Row> load(String... rows) throws IOException {
        Path csv = dir.resolve("data.csv");
        String content = HEADER + "\r\n" + String.join("\r\n", rows) + "\r\n";
        Files.write(csv, content.getBytes(StandardCharsets.ISO_8859_1));
        return OnlineRetail.loadSales(spark, csv.toString());
    }

    static String line(String invoice, String stockCode, String description, int quantity, double price, String customer) {
        return String.join(",", invoice, stockCode, description, String.valueOf(quantity),
                "12/1/2010 8:26", String.valueOf(price), customer, "United Kingdom");
    }

    static Row single(Dataset<Row> ds) {
        List<Row> rows = ds.collectAsList();
        assertEquals(1, rows.size());
        return rows.get(0);
    }
}
