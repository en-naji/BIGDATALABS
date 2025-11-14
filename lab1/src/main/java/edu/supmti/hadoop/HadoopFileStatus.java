package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        // Configuration Hadoop
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");

        FileSystem fs;
        try {
            fs = FileSystem.get(conf);

            // ===== LECTURE DES PARAMÈTRES =====
            if (args.length < 3) {
                System.out.println("Usage: hadoop jar HadoopFileStatus.jar <chemin> <nom_fichier> <nouveau_nom>");
                System.out
                        .println("Exemple: hadoop jar HadoopFileStatus.jar /user/root/input purchases.txt achats.txt");
                System.exit(1);
            }

            // Récupération des arguments
            String chemin = args[0]; // /user/root/input
            String nomFichier = args[1]; // purchases.txt
            String nouveauNom = args[2]; // achats.txt

            // Construire le chemin complet
            Path filepath = new Path(chemin, nomFichier);

            System.out.println("Chemin du fichier : " + filepath.toString());

            // ===== VÉRIFICATION DE L'EXISTENCE =====
            if (!fs.exists(filepath)) {
                System.out.println("ERREUR : Le fichier n'existe pas : " + filepath);
                System.exit(1);
            }

            // ===== RÉCUPÉRATION DES INFORMATIONS =====
            FileStatus status = fs.getFileStatus(filepath);

            System.out.println("\n=== INFORMATIONS DU FICHIER ===");
            System.out.println("Nom du fichier    : " + filepath.getName());
            System.out.println("Taille            : " + status.getLen() + " bytes");
            System.out.println("Propriétaire      : " + status.getOwner());
            System.out.println("Groupe            : " + status.getGroup());
            System.out.println("Permissions       : " + status.getPermission());
            System.out.println("Réplication       : " + status.getReplication());
            System.out.println("Taille des blocs  : " + status.getBlockSize() + " bytes");
            System.out.println("Date modification : " + status.getModificationTime());

            // ===== LOCALISATION DES BLOCS =====
            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            System.out.println("\n=== LOCALISATION DES BLOCS ===");
            System.out.println("Nombre de blocs : " + blockLocations.length);

            for (int i = 0; i < blockLocations.length; i++) {
                BlockLocation blockLocation = blockLocations[i];
                String[] hosts = blockLocation.getHosts();

                System.out.println("\nBloc " + (i + 1) + " :");
                System.out.println("  Offset  : " + blockLocation.getOffset());
                System.out.println("  Longueur: " + blockLocation.getLength());
                System.out.print("  Hébergé sur : ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }

            // ===== RENOMMAGE DU FICHIER =====
            Path newPath = new Path(chemin, nouveauNom);

            System.out.println("\n=== RENOMMAGE ===");
            System.out.println("Ancien nom : " + filepath.getName());
            System.out.println("Nouveau nom: " + nouveauNom);

            boolean renamed = fs.rename(filepath, newPath);

            if (renamed) {
                System.out.println("✓ Fichier renommé avec succès !");
            } else {
                System.out.println("✗ Échec du renommage");
            }

            fs.close();

        } catch (IOException e) {
            System.err.println("ERREUR : " + e.getMessage());
            e.printStackTrace();
        }
    }
}