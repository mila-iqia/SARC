# Dashboard SQL — Checklist de suivi du feedback utilisateur

- **Source** : `IDT-User dashboard feedback-120626-135459.pdf` (DRAFT, daté 2026-06-10/11)
- **Branche** : `dash-prototype-sql` · **Bilan dressé le** 2026-06-12
- **Fichiers** : `sarc/api/metrics.html` (frontend mono-fichier), `sarc/api/metrics.py` (endpoints `/dash/metrics/*`)

**Légende statut** : `[x]` fait · `[ ]` 🟡 partiel (le reste à faire est précisé) · `[ ]` à faire
**Provenance** : `(G)` feedback général · `(S10)` spec datée 2026-06-10 · `(SC)` Satya — défauts chercheur · `(SP)` Satya — défauts prof

> Plusieurs points avaient déjà été rattrapés par les commits du 11 juin (renommage *Efficiency metric*, Top-N déplacé, RGU by user used/unused triable). La checklist reflète l'état du code au 2026-06-12.

---

## ✅ Fait

- [x] **Chart d'évolution du `sm_occupancy`** — *Metric Trend per Period (mean & max)* `(G, S10-1)`
- [x] **RGU alloués dans le temps + gaspillage en volume** — *Total RGU per Period — Used vs Unused* (le « unused » = gaspillage volume) `(S10-1, Chart 2)`
- [x] **RGU by Cluster** (Used vs Unused, agrégé) `(S10)`
- [x] **Détail des jobs derrière un barchart** — *Job table* (pagination + tri serveur) reliée au **focus** (clic sur une barre → filtre la table) `(G, S10-3)`
- [x] **RGU by user** — Used / Unused / sans-métrique-en-gris, triable (requested/used/unused/unmeasured) `(SP)`
- [x] **Sélection des clusters** — tous / quelques-uns / un seul `(G)`
- [x] **Scope utilisateur** — tous (champ vide) ou **un seul** user `(S10-2a, S10-2c)`
- [x] **Top-N users déplacé au niveau du chart RGU by user** `(G, S10)`
- [x] **Period = menu déroulant contraignant** (radios hour/day/week/month + custom *Every N from Start*) `(G)`
- [x] **Semaine alignée au lundi** (mois au 1er) + tooltip `(G)`
- [x] **Period → RGU unit synchronisés** `(S10)`
- [x] **« Primary metric » → « Efficiency metric »** ; « Secondary metric » retirée de la barre (déplacée dans la tuile heatmap) `(G, S10)`
- [x] **Défaut Efficiency metric = SM occupancy** `(SC, SP)`
- [x] **Dashboard configurable (layout de tuiles) + sauvegarde** (localStorage) `(G — « se faire son propre dashboard ET le sauvegarder »)`
- [x] **Focus conserve l'heure** (`YYYY-MM-DDTHH:MM`) `(G)`
- [x] **Mouse-over explicatif sur tous les filtres** — tooltips ajoutés sur Start, End, User, Efficiency metric, Cluster, Job states (+ bouton Update) ; complètent Period / RGU type / RGU unit / Reset déjà présents `(G)`
- [x] **Titres des charts de métrique nomment la métrique** — Metric Trend → « SM occupancy per period — mean & max » et Density → « SM occupancy density — weighted by RGU·time » (dynamiques via `METRICS`, suivent l'Efficiency metric) ; le heatmap nommait déjà ses métriques `(S10 — Chart 2)`
- [x] **Tri par défaut Job table = RGU inutilisés (waste) desc** — `JOBTABLE_DEFAULTS.sortBy = 'waste'` (init + `resetState`) ; back-end déjà compatible (`waste = rgu_hours × (1 − mean)`, NULLs en dernier, suit l'Efficiency metric) `(SC, SP)`
- [x] **Clarté Primary/Secondary metric** — « Primary » → « Efficiency metric », « Secondary » retirée de la barre (devenue le picker « Compare … to: » du heatmap) + tooltip ; aucune occurrence visible de Primary/Secondary restante. Le masquage *Advanced* et le défaut Secondary sont des items distincts (Axe 1) `(G, S10)`
- [x] **Confusion Start/End vs Focus levée** — focus déjà transformé en label d'état (plus un champ) ; tooltip explicatif sur `#focus-label` (plage globale vs sous-tranche, tuiles filtrées : density / heatmap / RGU by user / job table) + message « No focus » enrichi pour décrire l'effet `(G)`
- [x] **Défaut Secondary metric (heatmap) = GPU utilization** — `metricScatterMetric2` initialisé à `'gpu_utilization'` (init via `??` pour préserver un « — none — » sauvegardé, + `resetState`) ; le heatmap par défaut compare donc SM occupancy × GPU utilization `(SC)`
- [x] **« RGU requested vs used » — vue agrégée (whole range)** — dropdown « View: Per period / Whole range » sur la tuile (`renderHistogram`) ; agrégé = une barre empilée sommant toute la plage Start–End, focus-agnostique (ni clic-focus, ni réaction à un focus posé ailleurs) ; somme côté front (pas de nouvel endpoint) ; mode persisté (`rguUsageView`) `(SC, SP)`
- [x] **Unité en % sur les charts de métrique** — `tickformat: '.0%'` sur les axes (Metric Trend Y, Density X, heatmap X/Y) + hovers en `.1%` ; données laissées en 0–1 (bins / range / focus intacts) ; la job table était déjà en % `(S10 — Chart 2)`

---

## 🟡 Partiellement fait

_(plus rien — tout est désormais soit fait, soit listé dans « À faire ».)_

---

## ❌ À faire

### Axe 1 — Onboarding & défauts par profil
- [ ] **Défauts de dates** : Period = `7d`, Start = `today-14d`, End = `today` (actuel : Jan 1 → lundi courant, période = semaine) `(SC, SP)`
- [ ] **Période relative à End** (`End -7d`) — le custom part de *Start* aujourd'hui (`_parse_period`, period custom) `(SC)`
- [ ] **Boutons raccourcis « 7 / 14 / 30 derniers jours »** `(SC, SP)`
- [ ] **User par défaut = l'utilisateur courant** (nécessite l'identité connectée) `(SC)`
- [ ] **Sélection multi-users (« User+s »)** — back-end en égalité stricte `cluster_user == <user>` `(SC)`
- [ ] **Section « Advanced »** masquant Focus + Primary/Secondary metric `(SC)`
- [ ] **Profils de layout chercheur vs prof** — un seul `DEFAULT_LAYOUT` aujourd'hui `(SC, SP)`

### Axe 2 — Scope « lab » / multi-users
- [ ] **Filtre « lab » / superviseur** (tous les users d'un prof) — `_apply_common_filters` / `_apply_slurm_job_filters` ne gèrent que `cluster_user` ; aucun rattachement lab côté back-end `(S10-2b, SP)`

### Axe 3 — Lisibilité & interaction des charts
- [ ] **Drill-down hiérarchique** (double-clic : mois → semaine → jour → heure) — seul le focus simple existe `(S10 — Charts 1 & 2)`
- [ ] **Étiquettes de valeurs lisibles sans hover** (+ arrondi à l'unité / au % près) — tout est en hover `(S10 — Charts all)`
- [ ] **Texte explicatif au-dessus de chaque chart** (ce qui est représenté + niveau de perf attendu / références) `(S10 — Charts all)`
- [ ] **Chart RGU by user scrollable** pour Top 100 sans casser le layout `(S10 — Chart 3)`
- [ ] **`Enter` = `Update`** sur la barre de filtres `(S10)`
- [ ] **Repenser les dropdowns de type de plot par tuile** (jugés confus ; viser 2–3 charts fixes) `(S10 — Charts all)`
- [ ] **Vérifier/corriger les timestamps des charts** — à préciser avec l'auteur du feedback `(S10)`

### Table
- [ ] **Colonnes redimensionnables** (lire tous les nœuds utilisés) — le show/hide de colonnes existe, pas le resize (`nodes` en ellipsis + hover) `(S10 — Table)`
- [ ] **(nice to have) Coloration des lignes** : used foncé / unused pâle, comme les barres `(SC, SP)`

### Nice to have
- [ ] **Highlight des 2 meilleurs / 2 pires users** dans RGU by user (matcher gaspilleurs vs meilleurs) `(SP)`

---

## Synthèse des priorités

La spec « simple » du 2026-06-10 (suivre RGU alloués + `sm_occupancy` dans le temps, scope tous/1-user, drill vers la table) est **essentiellement en place**. Le reste se concentre sur trois axes :

1. **Onboarding & défauts par profil** (Axe 1) — fort impact perçu, surtout front-end.
2. **Scope « lab »** (Axe 2) — le seul chantier nécessitant du back-end (rattachement user → lab/superviseur).
3. **Lisibilité & interaction des charts** (Axe 3) — labels sans hover, %, descriptions, drill-down.
