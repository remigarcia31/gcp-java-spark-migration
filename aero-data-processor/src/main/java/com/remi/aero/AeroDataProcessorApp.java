package com.remi.aero;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AeroDataProcessorApp {
    private static final Logger log = LoggerFactory.getLogger(AeroDataProcessorApp.class);

    public static void main(String[] args) {
        log.info("Démarrage de l'application Spark AeroDataProcessorApp v3 (avec args)");

        // Étape 0: Vérifier et récupérer les arguments de la ligne de commande
        if (args.length < 5) {
            log.error("ERREUR: Nombre d'arguments insuffisant.");
            log.error("Usage: <GCS_INPUT_PATH> <GCS_TEMP_BUCKET> <BQ_PROJECT_ID> <BQ_DATASET_NAME> <BQ_TABLE_NAME>");
            log.error("Exemple: gs://mon-bucket/raw_data/sample.csv mon-bucket mon-projet-id aero_data_results processed_flights");
            System.exit(1);
        }
        String gcsInputPath = args[0];
        String gcsTempBucket = args[1];
        String bqProjectId = args[2];
        String bqDatasetName = args[3];
        String bqTableName = args[4];

        log.info("Configuration reçue :");
        log.info("  Input Path: {}", gcsInputPath);
        log.info("  Temp Bucket: {}", gcsTempBucket);
        log.info("  BQ Project: {}", bqProjectId);
        log.info("  BQ Dataset: {}", bqDatasetName);
        log.info("  BQ Table: {}", bqTableName);


        SparkSession spark = null;
        try {
            spark = SparkSession.builder()
                    .appName("Aero Data Processor v3")
                    .getOrCreate();
            log.info("SparkSession initialisée.");

            // Étape 1: Lire depuis GCS
            log.info("Lecture depuis {}", gcsInputPath);
            Dataset<Row> inputDF = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(gcsInputPath); // Utilise la variable issue des args

            inputDF.printSchema();
            inputDF.show(5, false);

            // Étape 2: Transformation
            log.info("Transformation...");
            Dataset<Row> transformedDF = inputDF
                    .withColumnRenamed("nom", "aircraft_name")
                    .withColumn("processing_status", org.apache.spark.sql.functions.lit("PROCESSED_WITH_ARGS"));

            transformedDF.show(5, false);

            // Étape 3: Écrire dans BigQuery
            String fullBQTableName = String.format("%s.%s.%s", bqProjectId, bqDatasetName, bqTableName);
            log.info("Écriture dans BigQuery : {}", fullBQTableName);

            transformedDF.write()
                    .format("bigquery")
                    .option("temporaryGcsBucket", gcsTempBucket) // Utilise la variable issue des args
                    .option("table", fullBQTableName)          // Utilise la variable issue des args
                    .mode(SaveMode.Overwrite)
                    .save();

            log.info("Écriture dans BigQuery terminée.");

        } catch (Exception e) {
            log.error("Erreur pendant l'exécution Spark.", e);
            e.printStackTrace();
        } finally {
            if (spark != null) {
                log.info("Arrêt SparkSession.");
                spark.stop();
            }
        }
        log.info("Fin de l'application Spark AeroDataProcessorApp v3");
    }
}