package edu.ensias.hbase.tp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

public class HbaseSparkProcess {

    // La méthode 'createHbaseTable' contient en fait la logique Spark
    // (le nom est peut-être un peu trompeur, il ne crée pas de table ici)
    public void createHbaseTable() { // [cite: 207]

        // 1. Initialiser la configuration HBase
        Configuration config = HBaseConfiguration.create(); // [cite: 208]

        // 2. Configurer Spark
        SparkConf sparkConf = new SparkConf()
            .setAppName("SparkHBaseTest") // [cite: 216]
            .setMaster("local[4]"); // [cite: 216]
        
        // 3. Créer le contexte Spark
        JavaSparkContext jsc = new JavaSparkContext(sparkConf); // [cite: 217]

        // 4. Spécifier la table HBase à lire
        // On dit à Spark quelle table 'products' doit être lue
        config.set(TableInputFormat.INPUT_TABLE, "products"); // [cite: 218]

        // 5. Créer le RDD
        // On utilise newAPIHadoopRDD pour lire depuis HBase.
        // Le RDD sera un RDD de paires (Clé, Résultat)
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(
            config, // La configuration HBase
            TableInputFormat.class, // La classe 'InputFormat'
            ImmutableBytesWritable.class, // Classe de la clé (Row Key)
            Result.class // Classe de la valeur (la ligne HBase entière)
        ); // [cite: 219, 220]

        // 6. Exécuter une action sur le RDD
        // .count() est une action Spark qui va déclencher le job
        // et compter le nombre d'éléments (lignes) dans le RDD
        System.out.println("nombre d'enregistrements: " + hBaseRDD.count()); // [cite: 221]

        // N'oubliez pas de fermer le contexte Spark (bonne pratique)
        jsc.close();
    }

    // Le point d'entrée du programme
    public static void main(String[] args) { // [cite: 222]
        HbaseSparkProcess admin = new HbaseSparkProcess(); // [cite: 223]
        admin.createHbaseTable(); // [cite: 224]
    }
}