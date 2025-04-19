package com.remi.aero;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class AeroDataProcessorAppTest {

    private static SparkSession spark;

    // Méthode exécutée une seule fois avant tous les tests de cette classe
    @BeforeAll
    static void setupSparkSession() {
        // Crée une session Spark locale pour les tests
        spark = SparkSession.builder()
                .appName("AeroApp Unit Test")
                .master("local[2]") // Utilise 2 cœurs locaux
                .config("spark.sql.shuffle.partitions", "1") // Réduit le shuffle pour les tests locaux
                .getOrCreate();
    }

    // Méthode exécutée une seule fois après tous les tests de cette classe
    @AfterAll
    static void tearDownSparkSession() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    @DisplayName("Should calculate updated flight hours correctly")
    void testCalculateUpdatedAircraftStatus() {
        // 1 - Préparation des données de test

        // Schéma pour aircraftDF
        StructType aircraftSchema = new StructType(new StructField[]{
                DataTypes.createStructField("aircraft_id", DataTypes.StringType, false),
                DataTypes.createStructField("model", DataTypes.StringType, true),
                DataTypes.createStructField("initial_total_flight_hours", DataTypes.DoubleType, true)
        });
        // Données d'exemple
        List<Row> aircraftData = Arrays.asList(
                RowFactory.create("A001", "B737", 1000.0),
                RowFactory.create("A002", "A320", 2000.0),
                RowFactory.create("A003", "B777", 3000.0) // Avion sans vol récent
        );
        Dataset<Row> testAircraftDF = spark.createDataFrame(aircraftData, aircraftSchema);

        // Schéma pour recentHoursDF
        StructType recentHoursSchema = new StructType(new StructField[]{
                DataTypes.createStructField("aircraft_id", DataTypes.StringType, false),
                DataTypes.createStructField("recent_flight_hours", DataTypes.DoubleType, true)
        });
        // Données d'exemple
        List<Row> recentHoursData = Arrays.asList(
                RowFactory.create("A001", 50.5),
                RowFactory.create("A002", 100.0)
                // A003 n'a pas de vol récent dans cet exemple
        );
        Dataset<Row> testRecentHoursDF = spark.createDataFrame(recentHoursData, recentHoursSchema);

        // 2 - Appel de la méthode à tester
        Dataset<Row> resultDF = AeroDataProcessorApp.calculateUpdatedAircraftStatus(testAircraftDF, testRecentHoursDF);

        // 3 - Vérification des résultats
        System.out.println("Résultat du DataFrame calculé dans le test :");
        resultDF.show();

        // Vérif 1 - Le nombre de lignes doit être le même que l'input aircraftDF
        assertEquals(3, resultDF.count(), "Le nombre d'avions dans le résultat doit être 3");

        // Vérif 2 - Le schéma doit contenir les bonnes colonnes
        List<String> expectedColumns = Arrays.asList("aircraft_id", "model", "updated_total_flight_hours");
        assertThat(Arrays.asList(resultDF.columns())).containsExactlyInAnyOrderElementsOf(expectedColumns);

        // Vérif 3 - Vérifier le calcul pour un avion spécifique
        List<Row> resultRows = resultDF.collectAsList();

        // Trouve la ligne pour A001
        Optional<Row> aircraftA001 = resultRows.stream()
                .filter(row -> row.getString(row.fieldIndex("aircraft_id")).equals("A001"))
                .findFirst();
        assertTrue(aircraftA001.isPresent(), "L'avion A001 doit être présent");
        assertEquals(1050.5, aircraftA001.get().getDouble(aircraftA001.get().fieldIndex("updated_total_flight_hours")), 0.001, "Heures A001 incorrectes");

        // Vérif 4 - Vérifier le calcul pour un avion sans vol récent
        Optional<Row> aircraftA003 = resultRows.stream()
                .filter(row -> row.getString(row.fieldIndex("aircraft_id")).equals("A003"))
                .findFirst();
        assertTrue(aircraftA003.isPresent(), "L'avion A003 doit être présent");
        assertEquals(3000.0, aircraftA003.get().getDouble(aircraftA003.get().fieldIndex("updated_total_flight_hours")), 0.001, "Heures A003 incorrectes");
    }
}