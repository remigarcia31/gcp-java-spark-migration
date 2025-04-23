package com.remi.aero;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class AeroDataProcessorApp {

    private static final Logger log = LoggerFactory.getLogger(AeroDataProcessorApp.class);

    private static class Config {
        final String aircraftInputPath;
        final String flightsInputPath;
        final String gcsTempBucket;
        final String bqProjectId;
        final String bqDatasetName;
        final String bqOutputTableName;

        Config(String aircraftInputPath, String flightsInputPath, String gcsTempBucket, String bqProjectId, String bqDatasetName, String bqOutputTableName) {
            this.aircraftInputPath = aircraftInputPath;
            this.flightsInputPath = flightsInputPath;
            this.gcsTempBucket = gcsTempBucket;
            this.bqProjectId = bqProjectId;
            this.bqDatasetName = bqDatasetName;
            this.bqOutputTableName = bqOutputTableName;
        }
    }

    // Définition des Schémas
    static final StructType AIRCRAFT_SCHEMA = new StructType(new StructField[]{
            DataTypes.createStructField("aircraft_id", DataTypes.StringType, false),
            DataTypes.createStructField("model", DataTypes.StringType, true),
            DataTypes.createStructField("initial_total_flight_hours", DataTypes.DoubleType, true)
    });

    static final StructType FLIGHTS_SCHEMA = new StructType(new StructField[]{
            DataTypes.createStructField("flight_id", DataTypes.StringType, false),
            DataTypes.createStructField("aircraft_id", DataTypes.StringType, false),
            DataTypes.createStructField("flight_date", DataTypes.StringType, true),
            DataTypes.createStructField("flight_duration_hours", DataTypes.DoubleType, true)
    });


    // --- Méthodes Utilitaires ---

    /**
     * Parse et valide les arguments de la ligne de commande.
     * Retourne un objet Config ou null si invalide.
     */
    private static Config parseArguments(String[] args) {
        if (args.length < 6) {
            log.error("ERREUR: Nombre d'arguments insuffisant (6 attendus).");
            log.error("Usage: <aircraft_input_path> <flights_input_path> <gcs_temp_bucket> <bq_project_id> <bq_dataset_name> <bq_output_table_name>");
            return null;
        }
        return new Config(args[0], args[1], args[2], args[3], args[4], args[5]);
    }

    /**
     * Affiche la configuration utilisée
     */
    private static void logConfiguration(Config config) {
        log.info("Configuration utilisée :");
        log.info("  Aircraft Path: {}", config.aircraftInputPath);
        log.info("  Flights Path: {}", config.flightsInputPath);
        log.info("  Temp Bucket: {}", config.gcsTempBucket);
        log.info("  BQ Project: {}", config.bqProjectId);
        log.info("  BQ Dataset: {}", config.bqDatasetName);
        log.info("  BQ Table: {}", config.bqOutputTableName);
    }

    /**
     * Initialise et retourne une SparkSession
     */
    private static SparkSession initializeSparkSession() {
        log.info("Initialisation de la SparkSession...");
        SparkSession spark = SparkSession.builder()
                .appName("Aero Realistic Processing")
                .getOrCreate();
        log.info("SparkSession initialisée.");
        return spark;
    }

    /**
     * Lit un fichier CSV depuis GCS en appliquant un schéma.
     */
    private static Dataset<Row> readCsvWithSchema(SparkSession spark, String path, StructType schema, String description) {
        log.info("Lecture de '{}' depuis {}", description, path);
        Dataset<Row> df = spark.read()
                .schema(schema)
                .option("header", "true")
                .csv(path);
        log.info("Données '{}' lues ({} lignes). Aperçu :", description, df.count());
        df.show(5, false);
        return df;
    }

    /**
     * Calcule le total des heures de vol récentes par avion.
     */
     static Dataset<Row> calculateRecentHours(Dataset<Row> flightsDF) {
        log.info("Calcul des heures de vol récentes par avion...");
        Dataset<Row> recentHours = flightsDF
                .groupBy("aircraft_id")
                .agg(sum("flight_duration_hours").alias("recent_flight_hours"));
        log.info("Heures récentes calculées :");
        recentHours.show(5, false);
        return recentHours;
    }

    /**
     * Jointure des df et calcule le statut mis à jour des avions
     */
     static Dataset<Row> calculateUpdatedAircraftStatus(Dataset<Row> aircraftDF, Dataset<Row> recentHoursDF) {
        log.info("Calcul du nouveau total d'heures de vol...");
        Dataset<Row> updatedStatus = aircraftDF
                .join(recentHoursDF, aircraftDF.col("aircraft_id").equalTo(recentHoursDF.col("aircraft_id")), "left_outer")
                .withColumn("recent_hours_cleaned", coalesce(recentHoursDF.col("recent_flight_hours"), lit(0.0)))
                .withColumn("updated_total_flight_hours", expr("initial_total_flight_hours + recent_hours_cleaned"))
                .select(
                        aircraftDF.col("aircraft_id"),
                        aircraftDF.col("model"),
                        col("updated_total_flight_hours")
                );
        log.info("État mis à jour des avions calculé ({} lignes). Aperçu :", updatedStatus.count());
        updatedStatus.show(10, false);
        updatedStatus.printSchema(); // Vérifier le schéma final
        return updatedStatus;
    }

    /**
     * Écrit le df résultant dans BigQuery
     */
    private static void writeToBigQuery(Dataset<Row> resultsDF, String tempBucket, String projectId, String datasetName, String tableName) {
        String fullBQTableName = String.format("%s.%s.%s", projectId, datasetName, tableName);
        log.info("Écriture du résultat dans BigQuery : {}", fullBQTableName);

        resultsDF.write()
                .format("bigquery")
                .option("temporaryGcsBucket", tempBucket)
                .option("table", fullBQTableName)
                .mode(SaveMode.Overwrite)
                .save();
        log.info("Écriture dans BigQuery terminée.");
    }

    /**
     * Arrête la SparkSession si pas nulle
     */
    private static void stopSparkSession(SparkSession spark) {
        if (spark != null) {
            log.info("Arrêt SparkSession.");
            spark.stop();
        } else {
            log.warn("Tentative d'arrêt d'une SparkSession nulle.");
        }
    }

    public static void main(String[] args) {
        log.info("Démarrage de l'application Spark AeroDataProcessorApp v5");

        Config config = parseArguments(args);
        if (config == null) {
            System.exit(1); // stop si arguments invalides
        }
        logConfiguration(config);

        SparkSession spark = null;
        try {
            spark = initializeSparkSession();

            // Exécution du pipeline
            Dataset<Row> aircraftDF = readCsvWithSchema(spark, config.aircraftInputPath, AIRCRAFT_SCHEMA, "état initial des avions");
            Dataset<Row> flightsDF = readCsvWithSchema(spark, config.flightsInputPath, FLIGHTS_SCHEMA, "vols récents");

            Dataset<Row> recentHoursDF = calculateRecentHours(flightsDF);
            Dataset<Row> updatedStatusDF = calculateUpdatedAircraftStatus(aircraftDF, recentHoursDF);

            writeToBigQuery(updatedStatusDF, config.gcsTempBucket, config.bqProjectId, config.bqDatasetName, config.bqOutputTableName);

            log.info("Pipeline terminé avec succès.");

        } catch (Exception e) {
            log.error("Erreur majeure pendant l'exécution du pipeline Spark.", e);
            e.printStackTrace();
        } finally {
            stopSparkSession(spark);
        }
        log.info("Fin de l'application Spark AeroDataProcessorApp v5");
    }
}