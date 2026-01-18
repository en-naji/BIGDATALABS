package edu.ismagi.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// La classe Mapper prend 4 types génériques :
// 1. KEYIN : Object (Offset de la ligne dans le fichier, souvent ignoré)
// 2. VALUEIN : Text (La ligne de texte complète)
// 3. KEYOUT : Text (Le mot extrait)
// 4. VALUEOUT : IntWritable (Le nombre 1)
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    
    // Optimisation : On crée une constante "1" pour éviter de recréer l'objet à chaque fois
    private final static IntWritable one = new IntWritable(1);
    // Objet réutilisable pour stocker le mot en cours
    private Text word = new Text();

    // La méthode map est exécutée pour CHAQUE ligne du fichier d'entrée
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        // StringTokenizer découpe la ligne (value) en mots séparés par des espaces
        StringTokenizer itr = new StringTokenizer(value.toString());
        
        // Boucle sur chaque mot trouvé dans la ligne
        while (itr.hasMoreTokens()) {
            // Récupère le mot suivant et le met dans l'objet 'word'
            word.set(itr.nextToken());
            
            // Écrit la paire (Mot, 1) dans le contexte Hadoop
            // Exemple de sortie : (Chat, 1), (Chien, 1), (Chat, 1)
            context.write(word, one);
        }
    }
}