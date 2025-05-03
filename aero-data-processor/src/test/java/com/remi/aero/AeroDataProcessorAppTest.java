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
                .master("local[2]") // Utilise 2 coeurs locaux
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

    // Nouveau Test pour la méthode calculateRecentHours
    @Test
    @DisplayName("Should aggregate recent flight hours correctly per aircraft")
    void testCalculateRecentHours() {
        // Préparation des données de test pour flightsDF ---
        // Utilise le schéma FLIGHTS_SCHEMA
        List<Row> flightsData = Arrays.asList(
                RowFactory.create("F1001", "A001", "2025-04-18", 5.5),
                RowFactory.create("F1002", "A002", "2025-04-18", 3.0),
                RowFactory.create("F1003", "A001", "2025-04-19", 6.1), // 2ème vol pour A001
                RowFactory.create("F1004", "A003", "2025-04-19", 11.8),
                RowFactory.create("F1005", "A002", "2025-04-19", 3.2), // 2ème vol pour A002
                RowFactory.create("F1006", "A001", "2025-04-19", 1.5)  // 3ème vol pour A001
        );
        // Accède au schéma
        Dataset<Row> testFlightsDF = spark.createDataFrame(flightsData, AeroDataProcessorApp.FLIGHTS_SCHEMA);

        // appel de la méthode à tester
        Dataset<Row> resultDF = AeroDataProcessorApp.calculateRecentHours(testFlightsDF);

        // vérif des résultats
        System.out.println("Résultat du calcul des heures récentes dans le test :");
        resultDF.show();

        // Vérif 1: Le nombre de lignes doit être le nombre d'avions uniques ayant volé (3)
        assertEquals(3, resultDF.count(), "Le nombre d'avions uniques dans le résultat doit être 3");

        // Vérif 2: Le schéma doit être correct ("aircraft_id", "recent_flight_hours")
        List<String> expectedColumns = Arrays.asList("aircraft_id", "recent_flight_hours");
        assertThat(Arrays.asList(resultDF.columns())).containsExactlyInAnyOrderElementsOf(expectedColumns);

        // Vérif 3: Comparer les données calculées
        // ATTENTION: L'ordre après un groupBy/agg n'est pas garanti. Il faut soit trier,
        // soit comparer les ensembles sans tenir compte de l'ordre.
        List<Row> expectedData = Arrays.asList(
                RowFactory.create("A001", 5.5 + 6.1 + 1.5), // 13.1
                RowFactory.create("A002", 3.0 + 3.2),      // 6.2
                RowFactory.create("A003", 11.8)
        );

        // Collecte les résultats
        List<Row> actualData = resultDF.collectAsList();

        // Trier les deux listes par aircraft_id avant de comparer
        actualData.sort(java.util.Comparator.comparing(row -> row.getString(0)));
        expectedData.sort(java.util.Comparator.comparing(row -> row.getString(0)));

        // Compare les tailles
        assertEquals(expectedData.size(), actualData.size(), "Les listes de résultats n'ont pas la même taille");

        // Compare élément par élément
        for(int i=0; i < expectedData.size(); i++){
            assertEquals(expectedData.get(i).getString(0), actualData.get(i).getString(0), "Aircraft ID mismatch at index " + i); // Compare ID
            assertEquals(expectedData.get(i).getDouble(1), actualData.get(i).getDouble(1), 0.001, "Flight hours mismatch for " + expectedData.get(i).getString(0)); // Compare heures
        }

    }
}