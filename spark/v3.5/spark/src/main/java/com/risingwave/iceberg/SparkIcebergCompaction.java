package com.risingwave.iceberg;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SparkIcebergCompaction {
    public static void main(String[] args) {
        String catalog = args[0];
        String database = args[1];
        String table = args[2];
        SparkSession session = SparkSession.builder()
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config(String.format("spark.sql.catalog.%s", catalog), "org.apache.iceberg.spark.SparkCatalog")
                .getOrCreate();
        List<Row> rows = session.sql(String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('rewrite-all', 'true'))", catalog, database, table)).collectAsList();
        System.out.printf("compaction success: %s/%s, output: %s%n", database, table, rows);
        try {
            List<Row> expireSnapshotOutputRows = session.sql(String.format("CALL %s.system.expire_snapshots(table => '%s.%s')", catalog, database, table)).collectAsList();
            System.out.printf("expire_snapshots success: %s/%s, output: %s%n", database, table, expireSnapshotOutputRows);
        } catch (Throwable e) {
            System.err.printf("failed to run expire snapshots: %s%n", e);
        }

        try {
            List<Row> rewriteManifestsOutputRows = session.sql(String.format("CALL %s.system.rewrite_manifests(table => '%s.%s')", catalog, database, table)).collectAsList();
            System.out.printf("rewrite_manifests success: %s/%s, output: %s%n", database, table, rewriteManifestsOutputRows);
        } catch (Throwable e) {
            System.err.printf("failed to run rewrite manifests: %s%n", e);
        }

        try {
            List<Row> removeOrphanFilesOutputRows = session.sql(String.format("CALL %s.system.remove_orphan_files(table => '%s.%s')", catalog, database, table)).collectAsList();
            System.out.printf("remove_orphan_files success: %s/%s, output: %s%n", database, table, removeOrphanFilesOutputRows);
        } catch (Throwable e) {
            System.err.printf("failed to run remove orphan files: %s%n", e);
        }
    }
}
