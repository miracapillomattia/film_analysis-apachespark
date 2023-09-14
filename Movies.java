package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;


        

public class Movies {

	public static void main(String[] args) {

		
		SparkSession sparkSession = SparkSession.builder()
                .appName("Analisi dei Films")
                .master("local[*]") 
                .getOrCreate();

        String percorsoFile = "C:\\Users\\U-19\\eclipse-workspace\\ApacheSparkExam\\movies.csv"; 
        Dataset<Row> datiFilm = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(percorsoFile);
		
        
        //In questa porziona ho calcolato il numero totale di film dal file .csv
        long filmTotali = datiFilm.count();
        System.out.println("Il numero totale dei film è uguale a: " + filmTotali);
        
        // Qui ho calcolato il film con il rating ppiù alto
        Row filmPiuVotato = datiFilm.orderBy(col("Audience score %").desc()).first();
        String titoloFilmPiuVotato = filmPiuVotato.getAs("Film");
        int ratingPiuAlto = filmPiuVotato.getAs("Audience score %");
        System.out.println("Il film con il rating più alto è: " + titoloFilmPiuVotato + " || Il rating è: " + ratingPiuAlto);

        
        // in questa parte di codice ho messo i film in modalità tabellare con lo show() e ho calcolato il numero di film per ciascun genere
        Dataset<Row> filmPerGenere = datiFilm.groupBy("Genere").count();
        Dataset<Row> sortedfilmPerGenere = filmPerGenere.sort(col("Genere"));											                                	// Mostrare i risultati
        sortedfilmPerGenere.show(9999,false);

        
        // In questa porzione di codice ho messo i generein ordine alfabetico e ho calcolato il rating medio per ciascun genere
        Dataset<Row> ratingMedioPerGenere = datiFilm.groupBy("Genere").agg(avg("Audience score %"));
        Dataset<Row> sortedratingMedioPerGenere = ratingMedioPerGenere.sort(col("Genere"));											                                	// Mostrare i risultati
        sortedratingMedioPerGenere.show(9999,false);
        
    




        
        
        
        
        
        
        

               

          
        
        
        
        
        sparkSession.close();
    }
}
