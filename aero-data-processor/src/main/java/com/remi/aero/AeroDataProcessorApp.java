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

    // Schémas explicites pour nos fichiers CSV
    private static final StructType AIRCRAFT_SCHEMA = new StructType(new StructField[]{
            DataTypes.createStructField("aircraft_id", DataTypes.StringType, false),
            DataTypes.createStructField("model", DataTypes.StringType, true),
            DataTypes.createStructField("initial_total_flight_hours", DataTypes.DoubleType, true)
    });

    private static final StructType FLIGHTS_SCHEMA = new StructType(new StructField[]{
            DataTypes.createStructField("flight_id", DataTypes.StringType, false),
            DataTypes.createStructField("aircraft_id", DataTypes.StringType, false),
            DataTypes.createStructField("flight_date", DataTypes.StringType, true),
            DataTypes.createStructField("flight_duration_hours", DataTypes.DoubleType, true)
    });

    public static void main(String[] args) {
        log.info("Démarrage de l'application Spark AeroDataProcessorApp v4 (Realistic Processing)");

        // --- Arguments Attendus ---
        // args[0]: gs://<bucket>/path/to/aircraft_initial_state.csv
        // args[1]: gs://<bucket>/path/to/recent_flights.csv
        // args[2]: <bucket_name> (pour BQ temp)
        // args[3]: <project_id> (pour BQ)
        // args[4]: <dataset_name> (pour BQ)
        // args[5]: <output_table_name> (pour BQ, ex: aircraft_status_updated)
        // --------------------------

        if (args.length < 6) {
            log.error("ERREUR: Nombre d'arguments insuffisant (6 attendus).");
            log.error("Usage: <aircraft_input_path> <flights_input_path> <gcs_temp_bucket> <bq_project_id> <bq_dataset_name> <bq_output_table_name>");
            System.exit(1);
        }
        String aircraftInputPath = args[0];
        String flightsInputPath = args[1];
        String gcsTempBucket = args[2];
        String bqProjectId = args[3];
        String bqDatasetName = args[4];
        String bqOutputTableName = args[5];

        log.info("Config: Aircraft Path='{}', Flights Path='{}', Temp Bucket='{}', BQ Project='{}', BQ Dataset='{}', BQ Table='{}'",
                aircraftInputPath, flightsInputPath, gcsTempBucket, bqProjectId, bqDatasetName, bqOutputTableName);

        SparkSession spark = null;
        try {
            spark = SparkSession.builder()
                    .appName("Aero Realistic Processing")
                    .getOrCreate();
            log.info("SparkSession initialisée.");

            // Étape 1: Lire les données initiales des avions depuis GCS avec schéma
            log.info("Lecture de l'état initial des avions depuis {}", aircraftInputPath);
            Dataset<Row> aircraftDF = spark.read()
                    .schema(AIRCRAFT_SCHEMA) // Utilise le schéma défini
                    .option("header", "true")
                    .csv(aircraftInputPath);

            // Étape 2: Lire les données des vols récents depuis GCS avec schéma
            log.info("Lecture des vols récents depuis {}", flightsInputPath);
            Dataset<Row> flightsDF = spark.read()
                    .schema(FLIGHTS_SCHEMA) // Utilise le schéma défini
                    .option("header", "true")
                    .csv(flightsInputPath);

            log.info("Données initiales avions ({} lignes):", aircraftDF.count());
            aircraftDF.show(5, false);
            log.info("Vols récents ({} lignes):", flightsDF.count());
            flightsDF.show(5, false);

            // Étape 3: Calculer les heures de vol récentes par avion
            log.info("Calcul des heures de vol récentes par avion...");
            Dataset<Row> recentHoursPerAircraft = flightsDF
                    .groupBy("aircraft_id")
                    .agg(sum("flight_duration_hours").alias("recent_flight_hours")); // sum() importé statiquement

            log.info("Heures récentes calculées :");
            recentHoursPerAircraft.show(5, false);

            // Étape 4: Joindre les heures récentes avec l'état initial et calculer le nouveau total
            log.info("Calcul du nouveau total d'heures de vol...");
            Dataset<Row> updatedAircraftStatus = aircraftDF
                    .join(recentHoursPerAircraft, aircraftDF.col("aircraft_id").equalTo(recentHoursPerAircraft.col("aircraft_id")), "left_outer")
                    .withColumn("recent_hours_cleaned", coalesce(recentHoursPerAircraft.col("recent_flight_hours"), lit(0.0)))
                    .withColumn("updated_total_flight_hours",
                            expr("initial_total_flight_hours + recent_hours_cleaned")) // `expr` permet d'écrire comme en SQL
                    .select(
                            aircraftDF.col("aircraft_id"),
                            aircraftDF.col("model"),
                            col("updated_total_flight_hours")
                    );

            log.info("État mis à jour des avions ({} lignes):", updatedAircraftStatus.count());
            updatedAircraftStatus.show(10, false);
            log.info("Schema final avant écriture BQ :");
            updatedAircraftStatus.printSchema();


            // Étape 5: Écrire le résultat dans la nouvelle table BigQuery
            String fullBQTableName = String.format("%s.%s.%s", bqProjectId, bqDatasetName, bqOutputTableName);
            log.info("Écriture du résultat dans BigQuery : {}", fullBQTableName);

            updatedAircraftStatus.write()
                    .format("bigquery")
                    .option("temporaryGcsBucket", gcsTempBucket)
                    .option("table", fullBQTableName)
                    .mode(SaveMode.Overwrite) // Écrase la table à chaque fois pour ce test
                    .save();

            log.info("Écriture dans BigQuery terminée avec succès.");

        } catch (Exception e) {
            log.error("Erreur pendant l'exécution Spark.", e);
            e.printStackTrace();
        } finally {
            if (spark != null) {
                log.info("Arrêt SparkSession.");
                spark.stop();
            }
        }
        log.info("Fin de l'application Spark AeroDataProcessorApp v4");
    }
}