#!/usr/bin/env python
import sys

# Le script lit depuis l'entrée standard (STDIN)
# C'est ainsi que Hadoop lui envoie les données
for line in sys.stdin:
    # Enlever les espaces avant/après (strip)
    line = line.strip()
    
    # Découper la ligne en mots (split)
    words = line.split()
    
    # Pour chaque mot, émettre la paire (mot, 1)
    for word in words:
        # Le format est TRES important : clé \t valeur
        print('%s\t%s' % (word, 1))