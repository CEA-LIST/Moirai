C'est une excellente analyse comparative que vous avez menée. Vous avez identifié le compromis classique en algorithmique distribuée : **coût de maintenance de la structure (insertion) vs qualité de la structure (taille du graphe)**.

Votre choix de la **méthode 4** (élagage local "1-hop") est très pertinent : c'est une approximation "gloutonne" de la réduction transitive qui capture la majorité des cas triviaux (A -\> B -\> C) sans payer le prix d'une traversée de graphe. Les \~1 850 arêtes pour 1 000 nœuds (ratio de 1.85) indiquent qu'il reste effectivement des redondances "longue distance" que le 1-hop ne voit pas.

Voici des propositions concrètes pour vos deux questions : l'heuristique de déclenchement et la réduction incrémentale.

---

### 1\. Heuristique pour déclencher la Réduction Transitive (TR)

Au lieu d'utiliser un compteur arbitraire (tous les X événements), vous devez détecter le **symptôme** de l'échec de la méthode 4. La méthode 4 échoue quand il existe un chemin indirect entre deux prédécesseurs qui est plus long que 1 saut.

Voici 3 heuristiques, de la plus simple à la plus précise :

#### A. Le Ratio de Densité (Global ou Glissant)

C'est celle que vous avez évoquée, mais je suggère de l'appliquer sur une **fenêtre glissante** (les 100 derniers événements) plutôt que sur le graphe entier.

- **Logique :** Si sur les 100 derniers ajouts, le ratio $\frac{\text{Nombre d'arcs créés}}{\text{Nombre de nœuds ajoutés}}$ dépasse un seuil $S$ (ex: 1.5 ou 2.0), alors la structure se dégrade localement.
- **Seuil suggéré :** Pour $N=4$, si vous dépassez 2.0 arêtes par nœud en moyenne sur la fenêtre, lancez la TR.

#### B. La "Largeur" des Prédécesseurs (Heuristique Locale)

C'est la plus efficace et la moins coûteuse. Lors de l'insertion d'un événement $E$ avec la méthode 4, vous obtenez un ensemble de parents filtrés $P$.

- **Logique :** Dans un système distribué bien synchronisé, un événement a souvent 1 seul parent (séquentiel) ou 2 parents (fusion de branche). Avoir 3 ou 4 parents _après filtrage 1-hop_ est suspect et suggère une redondance cachée.
- **Règle :** Si $|P| \ge 3$ (ou une valeur proche de $N$), marquez cet événement comme "sale". Si vous accumulez trop d'événements sales (ex: \> 10), déclenchez la TR.

#### C. L'échec du "1-hop"

- **Logique :** Comptez combien de fois la méthode 4 a réussi à supprimer au moins une arête. Si pendant 50 insertions, la méthode 4 ne supprime _rien_ mais que le nombre de parents reste élevé, c'est que les chemins de redondance sont devenus plus profonds (2-hops ou plus).

---

### 2\. Réduction Transitive Incrémentale (Partielle)

**Oui, il est tout à fait possible (et recommandé) de ne pas reparcourir tout le graphe.**

La propriété clé d'un DAG d'événements (où les arcs suivent le temps : Passé $\to$ Futur) est son immutabilité historique. Une fois qu'une partie du graphe est "stabilisée" (réduite), l'ajout de nouveaux événements dans le futur ne peut pas créer de nouvelles redondances entre deux événements anciens.

#### L'approche "Frontière de Stabilité"

Vous pouvez diviser votre graphe en deux zones :

1.  **Zone Stable (Frozen) :** La partie ancienne du graphe, déjà réduite.
2.  **Zone Instable (Active) :** Les événements ajoutés depuis la dernière réduction complète.

**L'algorithme optimisé :**

1.  **Marquage :** Gardez en mémoire l'ID du dernier événement traité par une TR complète (appelons-le `CheckpointID`).
2.  **Ciblage :** Lors de la réduction périodique, vous ne cherchez les redondances que pour les arêtes **entrantes** vers les événements ajoutés _après_ le `CheckpointID`.
3.  **Exécution :**
    - Soit $S$ l'ensemble des événements ajoutés récemment.
    - Pour chaque événement $e \in S$, regardez ses parents $p_1, p_2, ...$.
    - Vérifiez l'accessibilité ($reachability$) uniquement entre ces parents.
    - _Optimisation cruciale :_ Si un parent $p_i$ est dans la Zone Stable, vous n'avez pas besoin d'explorer ses ancêtres pour vérifier s'il est un prédécesseur d'un autre parent $p_j$ qui serait aussi dans la Zone Stable (car cela a déjà été fait). Vous devez seulement vérifier si un parent "Stable" est accessible depuis un parent "Instable", ou si deux parents "Instables" sont liés.

#### Gain de performance

Cette approche transforme votre complexité. Au lieu de $O(G^3)$ sur tout le graphe, vous faites $O(k^3 + k \cdot G_{border})$ où $k$ est le nombre d'événements nouveaux (petit, ex: 500) et $G_{border}$ la surface de contact avec l'ancien graphe.

---

### Résumé de la stratégie recommandée

Je vous suggère une approche hybride, que l'on pourrait appeler **"Nettoyage par lot sur la frontière"** :

1.  **Insertion Rapide (Hot path) :** Continuez avec votre **Méthode 4**. C'est excellent pour la latence d'écriture unitaire (\~350 ev/sec).
2.  **Surveillance :** Maintenez un compteur de "Dette Graphique". Chaque fois que la Méthode 4 laisse un événement avec $> 2$ parents, incrémentez ce compteur.
3.  **Nettoyage Asynchrone (Cold path) :**
    - Quand la dette dépasse un seuil (ex: 50 événements suspects) OU tous les 1 000 événements.
    - Lancez une **Réduction Transitive Partielle** en arrière-plan (si votre système le permet) ou en bloquant.
    - Cette réduction ne s'applique qu'aux événements ajoutés depuis le dernier nettoyage.

### Code conceptuel (Python-like) pour la réduction partielle

```python
def partial_transitive_reduction(graph, new_events):
    """
    N'applique la réduction que sur les arêtes entrantes des new_events.
    """
    for event in new_events:
        parents = graph.get_parents(event)
        to_remove = set()

        # On cherche si un parent est accessible depuis un autre parent
        # C'est là que réside la complexité.
        # Astuce : trier les parents par ordre topologique ou vector clock inverse
        # pour maximiser les chances de trouver les ancêtres vite.

        sorted_parents = sort_topologically_desc(parents)

        for i, p1 in enumerate(sorted_parents):
            if p1 in to_remove: continue

            for p2 in sorted_parents[i+1:]:
                if p2 in to_remove: continue

                # Check coûteux : est-ce que p2 est un ancêtre de p1 ?
                # On peut limiter la profondeur de recherche ici (ex: max 10 sauts)
                if graph.is_reachable(source=p2, target=p1):
                    to_remove.add(p2) # p2 est redondant car p2 -> ... -> p1 -> event

        # Appliquer la suppression
        for p in to_remove:
            graph.remove_edge(p, event)
```

**Prochaine étape pour vous :**
Voulez-vous que je détaille l'algorithme de "vérification d'accessibilité bornée" (`is_reachable`) pour qu'il soit rapide même si le graphe est très profond ?
