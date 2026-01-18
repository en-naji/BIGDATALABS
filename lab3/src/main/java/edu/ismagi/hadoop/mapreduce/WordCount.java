package edu.ismagi.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    public static void main(String[] args) throws Exception {
        // Configuration de base Hadoop
        Configuration conf = new Configuration();
        
        // Création de l'instance du Job
        Job job = Job.getInstance(conf, "word count");
        
        // Indique à Hadoop dans quel fichier JAR chercher les classes Mapper et Reducer
        job.setJarByClass(WordCount.class);
        
        // Définit quelle classe utiliser pour le Mapping
        job.setMapperClass(TokenizerMapper.class);
        
        // Le Combiner est un "mini-reducer" qui tourne localement avant l'envoi réseau
        // Cela optimise les performances en réduisant le trafic réseau
        job.setCombinerClass(IntSumReducer.class);
        
        // Définit quelle classe utiliser pour le Reducing final
        job.setReducerClass(IntSumReducer.class);
        
        // Définit les types de données de sortie finale (Clé = Texte, Valeur = Entier)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Définit le chemin du fichier d'entrée (args[0] = premier argument commande)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        // Définit le chemin du dossier de sortie (args[1] = deuxième argument commande)
        // ATTENTION : Ce dossier ne doit pas exister avant l'exécution
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // Lance le job et attend qu'il finisse (affiche true si succès, false sinon)
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}