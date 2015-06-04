# Breizhcamp 2015
# Débuter avec apache Storm

Projet créé pour la conférence Débuter avec Apache Storm pour le BreizhCamp 2015.

# Sheep-counter

Sheep-counter est une topologie Storm pour trier et compter par couleur les moutons envoyés sur un pub/sub Redis.

Pour la lancer : 
  - exécuter la classe ```bzh.cjuste.breizhcamp.sheepcounter.SheepCounterTopology```
  - attendre le message ```Ready to receive messages !```
  - exécuter la classe de test (dans src/test/java) ```bzh.cjuste.breizhcamp.sheepcounter.SendSheeps```
