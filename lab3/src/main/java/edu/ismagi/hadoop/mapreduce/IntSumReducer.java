package edu.ismagi.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

// Le Reducer prend :
// 1. KEYIN : Text (Le mot)
// 2. VALUEIN : IntWritable (La liste des 1 venant des mappers : [1, 1, 1...])
// 3. KEYOUT : Text (Le mot)
// 4. VALUEOUT : IntWritable (La somme totale)
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable();

    // La méthode reduce est exécutée UNE FOIS par clé unique (mot unique)
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        
        // Parcourt la liste des valeurs pour ce mot (ex: pour "Chat", values = [1, 1, 1])
        for (IntWritable val : values) {
            sum += val.get(); // Ajoute la valeur à la somme
        }
        
        // Met à jour l'objet résultat avec la somme finale
        result.set(sum);
        
        // Écrit le résultat final sur HDFS : (Mot, Somme)
        // Exemple : (Chat, 3)
        context.write(key, result);
    }
}