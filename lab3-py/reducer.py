#!/usr/bin/env python
import sys

current_word = None
current_count = 0
word = None

# Lit ligne par ligne depuis STDIN
for line in sys.stdin:
    line = line.strip()

    # Tente de séparer le mot et le compteur
    try:
        word, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        # Ignore la ligne si elle est mal formatée
        continue

    # Si on est toujours sur le même mot, on additionne
    if current_word == word:
        current_count += count
    else:
        # Si le mot change, on affiche le total du mot précédent
        if current_word:
            print('%s\t%s' % (current_word, current_count))
        # Et on commence à compter le nouveau mot
        current_count = count
        current_word = word

# Ne pas oublier d'afficher le dernier mot après la fin de la boucle
if current_word == word:
    print('%s\t%s' % (current_word, current_count))