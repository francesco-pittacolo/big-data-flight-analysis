"""
Generazione grafici di confronto tempi di esecuzione
Task 3.1, 3.2, 3.3 — Spark Core, Spark SQL, Hive

Output:
  - grafici singoli:   single/task_{t}_{fw}_{metric}.png
  - grafici per task:  grouped/task_{t}_all_frameworks.png
  - confronto fw:      grouped/confronto_framework_6nodi.png
  - speedup:           grouped/speedup_vs_locale.png
"""

import os
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SINGLE_DIR = os.path.join(BASE_DIR, "single")
GROUPED_DIR = os.path.join(BASE_DIR, "grouped")

os.makedirs(SINGLE_DIR, exist_ok=True)
os.makedirs(GROUPED_DIR, exist_ok=True)

# Palette e stile
COLORS  = {"Locale":"#2C3E50","2 nodi":"#E74C3C","4 nodi":"#3498DB","6 nodi":"#27AE60"}
MARKERS = {"Locale":"o","2 nodi":"s","4 nodi":"^","6 nodi":"D"}
CONFIGS  = ["Locale","2 nodi","4 nodi","6 nodi"]
DATASETS = [25,50,75,100,200,400]

plt.rcParams.update({
    "font.family":"DejaVu Sans","font.size":10,
    "axes.titlesize":12,"axes.titleweight":"bold","axes.labelsize":10,
    "axes.spines.top":False,"axes.spines.right":False,
    "axes.grid":True,"grid.alpha":0.35,"grid.linestyle":"--",
    "legend.framealpha":0.85,"figure.dpi":150,
})

# Dati
data = {
    "3.1": {
        "Spark Core": {
            "job":  {"Locale":[12,17,23,28,48,90],"2 nodi":[36,42,52,54,90,145],
                     "4 nodi":[33,38,42,49,78,103],"6 nodi":[35,36,43,45,57,80]},
            "task": {"Locale":[8.1,14.1,19.2,24.1,45.1,85.2],"2 nodi":[19.3,24.4,30.4,36.4,71.7,125.2],
                     "4 nodi":[16.2,21.5,24.2,30.4,58.4,84.6],"6 nodi":[18.2,19.3,22.5,24.6,40.5,62]},
        },
        "Spark SQL": {
            "job":  {"Locale":[7,7,8,8,9,12],"2 nodi":[29,29,31,32,34,39],
                     "4 nodi":[22,22,23,23,25,29],"6 nodi":[20,21,21,21,22,26]},
            "task": {"Locale":[3.3,3.6,4.4,4.5,5.4,9.1],"2 nodi":[12.9,12.8,13.4,13.8,17,20.9],
                     "4 nodi":[11.7,12.1,12.7,14.1,15,17.9],"6 nodi":[11.8,11.9,11.9,11.9,13,17.1]},
        },
        "Hive": {
            "job":  {"Locale":[12,13,17,16,22,34],"2 nodi":[40,44,46,45,49,53],
                     "4 nodi":[32,43,37,37,44,51],"6 nodi":[43,37,38,38,46,50]},
            "task": {"Locale":[9.3,10.2,14.1,13.3,19.2,31.4],"2 nodi":[27.7,32.3,32.3,32.8,37.3,41.1],
                     "4 nodi":[21.4,31.1,26.4,24.9,32.5,39.8],"6 nodi":[25.8,23.7,25.9,25.9,34.6,37.7]},
        },
    },
    "3.2": {
        "Spark Core": {
            "job":  {"Locale":[11,16,21,26,44,84],"2 nodi":[34,41,46,53,82,132],
                     "4 nodi":[32,41,43,45,62,95],"6 nodi":[31,34,38,42,54,77]},
            "task": {"Locale":[8,13,18.1,23,41,79.1],"2 nodi":[17.7,22.6,28.7,33.6,64.8,119.8],
                     "4 nodi":[14.6,21.9,24.8,27.7,44.8,76.8],"6 nodi":[12.5,16.6,20.8,24.6,35.7,58]},
        },
        "Spark SQL": {
            "job":  {"Locale":[8,8,8,9,9,11],"2 nodi":[28,30,31,31,30,32],
                     "4 nodi":[22,23,22,23,24,25],"6 nodi":[20,21,21,21,22,26]},
            "task": {"Locale":[6.9,7,7.1,8.4,8.5,10.2],"2 nodi":[17.9,18.9,18.9,19.2,21.8,25.9],
                     "4 nodi":[13,14.4,14.5,16.1,16.3,17.6],"6 nodi":[11.7,12.8,12.6,12.7,14.6,17.8]},
        },
        "Hive": {
            "job":  {"Locale":[24,27,30,35,49,76],"2 nodi":[59,55,55,61,62,69],
                     "4 nodi":[55,52,57,63,57,63],"6 nodi":[54,56,58,57,65,62]},
            "task": {"Locale":[21.1,23.9,26.9,31.9,46.1,73.7],"2 nodi":[46.9,42.3,43.1,48.2,48.7,56.3],
                     "4 nodi":[42.6,41.2,45.9,50.5,45.1,51.9],"6 nodi":[42,43.2,46.9,44.7,53.1,50.9]},
        },
    },
    "3.3": {
        "Spark Core": {
            "job":  {"Locale":[16,25,35,45,84,162],"2 nodi":[40,51,60,74,124,214],
                     "4 nodi":[40,45,53,62,108,147],"6 nodi":[35,39,47,53,75,111]},
            "task": {"Locale":[12.1,22,32,42.1,81.1,157.1],"2 nodi":[23.1,32,44.1,54.7,105.7,196.9],
                     "4 nodi":[20.9,26.7,35.9,42,90.8,140.9],"6 nodi":[15.9,22.8,27.9,35.2,55.9,94.1]},
        },
        "Spark SQL": {
            "job":  {"Locale":[6,6,6,6,6,7],"2 nodi":[27,27,28,27,27,29],
                     "4 nodi":[19,19,19,20,20,21],"6 nodi":[18,18,18,18,19,21]},
            "task": {"Locale":[4.1,3.2,4.1,4.1,4.2,5.2],"2 nodi":[12.6,12.3,12.5,13.6,14.6,17.4],
                     "4 nodi":[13,14.4,14.5,16.1,16.3,17.6],"6 nodi":[13.6,12.9,13.3,13.7,14,15.8]},
        },
        "Hive": {
            "job":  {"Locale":[20,20,22,23,29,40],"2 nodi":[39,44,44,44,47,50],
                     "4 nodi":[31,35,40,41,43,45],"6 nodi":[32,36,35,35,45,49]},
            "task": {"Locale":[17.7,17.6,19.7,20.7,26.6,37.7],"2 nodi":[25.9,31.8,31.7,31.8,33.9,38.2],
                     "4 nodi":[19.8,24.4,28.2,29.8,32.2,34.2],"6 nodi":[19.2,23.1,22.8,23.4,33.9,37.2]},
        },
    },
}

FRAMEWORKS = ["Spark Core","Spark SQL","Hive"]
TASKS      = ["3.1","3.2","3.3"]
METRICS    = [("job","Job Duration (s)"),("task","Task Time (s)")]

METRIC_LABEL = {"job":"Job Duration","task":"Task Time"}

# ─── Helper base────
def _draw(ax, task_id, framework, metric_key, metric_label, show_legend=True):
    vals = data[task_id][framework][metric_key]
    for cfg in CONFIGS:
        ax.plot(DATASETS, vals[cfg],
                color=COLORS[cfg], marker=MARKERS[cfg],
                linewidth=2, markersize=6, label=cfg)
    ax.set_xlabel("Dataset size (% rispetto al base)")
    ax.set_ylabel(metric_label)
    ax.set_xticks(DATASETS)
    ax.xaxis.set_minor_locator(ticker.NullLocator())
    if show_legend:
        ax.legend(title="Configurazione", fontsize=8, title_fontsize=8)


# SEZIONE 1 — Grafici singoli (1 grafico = 1 framework × 1 metrica × 1 task)
print("=== Grafici singoli ===")
for task_id in TASKS:
    for fw in FRAMEWORKS:
        for mkey, mlabel in METRICS:
            fig, ax = plt.subplots(figsize=(7, 4.5), constrained_layout=True)
            _draw(ax, task_id, fw, mkey, mlabel)
            ax.set_title(f"Task {task_id} — {fw} — {METRIC_LABEL[mkey]}")
            fw_safe = fw.replace(" ","_")
            fname = os.path.join(
                SINGLE_DIR,
                f"task_{task_id}_{fw_safe}_{mkey}.png"
            )
            fig.savefig(fname, bbox_inches="tight")
            print(f"  {fname}")
            plt.close(fig)


# SEZIONE 2 — Grafici raggruppati per task (griglia 2×3: metrica × framework)
print("\n=== Grafici per task ===")
for task_id in TASKS:
    fig, axes = plt.subplots(2, 3, figsize=(15, 8), constrained_layout=True)
    fig.suptitle(f"Task {task_id} — Tempi di esecuzione al variare del dataset",
                 fontsize=14, fontweight="bold")

    for col, fw in enumerate(FRAMEWORKS):
        for row, (mkey, mlabel) in enumerate(METRICS):
            ax = axes[row, col]
            _draw(ax, task_id, fw, mkey, mlabel, show_legend=(col == 2))
            ax.set_title(fw)
            if row == 0:
                ax.set_xlabel("")   # pulisce asse x sulla riga superiore

    # etichette di riga a sinistra
    for row, (_, mlabel) in enumerate(METRICS):
        axes[row, 0].annotate(
            mlabel,
            xy=(0, 0.5), xytext=(-axes[row,0].yaxis.labelpad - 15, 0),
            xycoords=axes[row,0].yaxis.label, textcoords="offset points",
            size=11, ha="right", va="center", fontweight="bold", rotation=90,
        )

    fname = os.path.join(
        GROUPED_DIR,
        f"task_{task_id}_all_frameworks.png"
    )
    fig.savefig(fname, bbox_inches="tight")
    print(f"  {fname}")
    plt.close(fig)

print("\nDone.")
