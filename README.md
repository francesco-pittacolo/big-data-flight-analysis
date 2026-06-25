# Big Data Flight Analysis

Progetto di analisi Big Data su dati di voli basato su **Apache Spark** e **Apache Hive**, con supporto all'esecuzione sia locale (HDFS) che su cluster AWS (S3 + EMR).

---

## Indice

- [Requisiti](#requisiti)
- [Configurazione](#configurazione)
- [Struttura del progetto](#struttura-del-progetto)
- [Dataset](#dataset)
- [Preprocessing](#preprocessing)
- [Analisi Implementate](#analisi-implementate)
- [Analisi Spark](#analisi-spark)
- [Analisi Hive](#analisi-hive)
- [Output](#output)
- [Log](#log)
- [Upload su AWS S3](#upload-su-aws-s3)

---

## Requisiti

### Dipendenze Python

```bash
pip install -r requirements.txt
```

| Pacchetto   | Versione |
|-------------|----------|
| pyspark     | 3.5.8    |
| pandas      | latest   |
| numpy       | latest   |
| boto3       | latest   |

### Tecnologie

- Python 3.x
- Java 11
- Apache Spark 3.5.8 / PySpark 3.5.8
- Apache Hive
- Hadoop HDFS
- AWS S3 + EMR (modalità cluster)

---

## Configurazione

### Nome del bucket S3

Il nome del bucket S3 è definito come variabile `S3_BUCKET` (o `BUCKET`) in tutti gli script (`run_spark.sh`, `run_hive.sh`, `run_all_hive.sh`, `upload_to_s3/upload_all.py`, `preprocessing.py`). Assicurarsi che corrisponda al bucket del proprio account AWS prima di eseguire in modalità cluster:

```bash
S3_BUCKET="big-data-2026-project"  # modificare se necessario
```
## Dataset

Il dataset sorgente è `flight_data_2024.csv`, contenente dati di voli negli USA per l'anno 2024.

> **Il dataset non è incluso nella repository** a causa delle sue dimensioni (1.2 GB). Va scaricato manualmente da Kaggle e posizionato in `data/flight_data_2024.csv`:
> [https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024)

---

## Struttura del progetto

```
big-data-flight-analysis/
│
├── src/
│   ├── preprocessing/
│   │   └── preprocessing.py          # Pulizia e generazione dataset scalati
│   │
│   ├── analysis/
│   │   ├── spark/
│   │   │   ├── task_3_1/
│   │   │   │   ├── 3_1_spark_core.py  # RDD API: statistiche per rotta/vettore
│   │   │   │   └── 3_1_spark_sql.py   # Spark SQL: stessa analisi
│   │   │   ├── task_3_2/
│   │   │   │   ├── 3_2_spark_core.py  # RDD API: analisi ritardi per fascia oraria
│   │   │   │   └── 3_2_spark_sql.py   # Spark SQL: stessa analisi
│   │   │   └── task_3_3/
│   │   │       ├── 3_3_spark_core.py  # RDD API: analisi cancellazioni
│   │   │       └── 3_3_spark_sql.py   # Spark SQL: stessa analisi
│   │   │
│   │   └── hive/
│   │       ├── 3_1_hive.sql           # Statistiche per rotta (carrier, origin, dest)
│   │       ├── 3_2_hive.sql           # Analisi ritardi per fascia di partenza
│   │       └── 3_3_hive.sql           # Analisi cancellazioni per mese/aeroporto
│   │
│   └── inspect/
│       ├── inspect_task_3_1.py        # Ispezione output Task 3.1
│       ├── inspect_task_3_2.py        # Ispezione output Task 3.2
│       ├── inspect_task_3_3.py        # Ispezione output Task 3.3
│       └── inspect_job_outputs.py     # Utility generica per output jobs
│ 
│ 
├── plots/
│   ├── generate_plots.py             # Generazione grafici delle performance
│   ├── single/                       # Grafici singoli
│   └── grouped/                      # Grafici aggregati
│ 
│ 
├── scripts/
│   ├── run_spark.sh                   # Esegue job Spark (singolo task)
│   ├── run_hive.sh                    # Esegue script Hive (singolo task)
│   └── run_all_hive.sh               # Esegue tutti i task Hive in sequenza
│
├── upload_to_s3/
│   ├── upload_file.py                 # Upload di un singolo file su S3
│   └── upload_all.py                  # Upload dell'intero progetto su S3
│
├── logs/
│   ├── spark/                         # Log esecuzioni Spark (per task e size)
│   └── hive/                          # Log esecuzioni Hive (per task e size)
│
├── data/                              # Dataset locali (non versionati)
├── requirements.txt
├── technical_report.pdf
└── .gitignore
```

---



---

## Preprocessing

Lo script `preprocessing.py` legge il CSV grezzo, esegue la pulizia dei dati e produce versioni del dataset scalate in formato Parquet.

### Esecuzione

**Locale:**
```bash
python src/preprocessing/preprocessing.py
```

**Cluster:**
```bash
python src/preprocessing/preprocessing.py -c
```

### Path dataset pulito

**Locale / HDFS:**
```
hdfs:///big_data/data/processed/flights_cleaned_<SIZE>.parquet
```

**Cluster (S3):**
```
s3a://big-data-2026-project/data/processed/flights_cleaned_<SIZE>.parquet
```

### Taglie disponibili

| `<SIZE>` | Fattore di scala |
|----------|-----------------|
| `25`     | 25% del dataset originale |
| `50`     | 50% |
| `75`     | 75% |
| `100`    | Dataset completo (1×) |
| `200`    | Dataset replicato 2× |
| `400`    | Dataset replicato 4× |

---

## Analisi Implementate

Le analisi sono implementate sia in Apache Spark (Spark Core e Spark SQL) che in Apache Hive, e comprendono tre task:
### Task 3.1 — Statistiche delle compagnie aeree

Per ogni compagnia aerea, genera le statistiche di ciascuna tratta o aeroporto di partenza servito: numero di voli, ritardo di arrivo minimo/massimo/medio, tasso di cancellazione ed elenco dei mesi in cui la compagnia opera su quella tratta.

### Task 3.2 — Report dei ritardi per aeroporto e periodo temporale

Per ogni aeroporto di partenza e per ogni mese, produce un report con: il numero di voli suddivisi in tre fasce di ritardo in partenza (basso < 15 min, medio 15–60 min, alto > 60 min), il ritardo medio in partenza e in arrivo per ciascuna fascia, e le tre cause di cancellazione o ritardo più frequenti.

### Task 3.3 — Ranking delle coppie compagnia-aeroporto con comportamento anomalo

Per ogni coppia (aeroporto di partenza, compagnia aerea), confronta le performance della compagnia con la media di tutte le compagnie che operano nello stesso aeroporto. Il report include: numero di voli, ritardo medio in partenza e in arrivo, tasso di cancellazione, scarto rispetto alla media dell'aeroporto e posizione in classifica per ritardo medio in partenza (dalla migliore alla peggiore).

---

## Analisi Spark

Ogni task è implementato in due varianti: **Spark Core** (RDD API) e **Spark SQL** (DataFrame + SQL), parametrizzate per taglia del dataset e modalità di esecuzione.

### Prerequisito — Avviare HDFS

Prima di eseguire qualsiasi job Spark in locale, avviare il filesystem distribuito:

```bash
$HADOOP_HOME/sbin/start-dfs.sh
```

---

### Esecuzione Spark

Usa lo script `run_spark.sh`, che supporta i flag `-c` (cluster) e `-m` (implementazione).

**Locale — Spark Core:**
```bash
./scripts/run_spark.sh -m spark_core
```

**Locale — Spark SQL:**
```bash
./scripts/run_spark.sh -m spark_sql
```

**Cluster:**

1. Collegarsi via SSH al nodo master del cluster
2. Caricare i file su S3 se non è già stato fatto (vedi [Upload su AWS S3](#upload-su-aws-s3))
3. Copiare lo script dalla macchina:
```bash
aws s3 cp s3://big-data-2026-project/scripts/run_spark.sh .
```
4. Renderlo eseguibile:
```bash
chmod +x run_spark.sh
```
5. Eseguirlo:
```bash
./run_spark.sh -c -m spark_core
./run_spark.sh -c -m spark_sql
```

**Ulteriori informazioni**
> Per eseguire solo un certo sottogruppo, editare le variabili `TASKS` e `SIZES` in `run_spark.sh`.

Per eseguire una singola task è possibile richiamare direttamente lo script Python corrispondente:

```bash
spark-submit src/analysis/spark/task_3_1/3_1_spark_core.py -s 100         # locale
spark-submit src/analysis/spark/task_3_1/3_1_spark_core.py -s 100 -c      # cluster
```

---

## Analisi Hive

Gli script Hive replicano le stesse analisi dei task Spark usando HiveQL, parametrizzati tramite `hivevar` per la taglia del dataset e i path di input/output.

### Prerequisito — Configurare il path di Hive

In `run_hive.sh` e `run_all_hive.sh` è presente la variabile `LOCAL_HIVE_CMD` che punta all'eseguibile Hive locale. Va modificata in base al proprio sistema prima di eseguire gli script:

```bash
# DA CAMBIARE IN BASE AI PROPRI PATH
LOCAL_HIVE_CMD="/home/ubuntu/apache-hive-2.3.9-bin/bin/hive"
```

### Prerequisito — Inizializzare il metastore

Al primo utilizzo, bisogna entrare nella cartella `scripts` ed inizializzare il metastore Derby con il comando:

```bash
schematool -dbType derby -initSchema
```

### Esecuzione Hive

**Tutti i task in sequenza — Locale:**
```bash
./scripts/run_all_hive.sh
```

**Singolo task — Locale:**
```bash
./scripts/run_hive.sh -t 3_1 -s 100
```


**Cluster:**

1. Collegarsi via SSH al nodo master del cluster
2. Caricare i file su S3 se non è già stato fatto (vedi [Upload su AWS S3](#upload-su-aws-s3))
3. Copiare lo script dalla macchina:
```bash
aws s3 cp s3://big-data-2026-project/scripts/run_all_hive.sh .
# oppure per eseguire una sola task:
aws s3 cp s3://big-data-2026-project/scripts/run_hive.sh .
```
4. Renderlo eseguibile:
```bash
chmod +x run_all_hive.sh
# oppure:
chmod +x run_hive.sh
```
5. Eseguirlo:
```bash
./run_all_hive.sh -c
# oppure per una specifica task:
./run_hive.sh -c -t 3_1 -s 100
```

I task disponibili sono `3_1`, `3_2`, `3_3`.

---

## Output

I risultati vengono salvati come file Parquet con la struttura seguente:

**Locale / HDFS:**
```
hdfs:///big_data/results/task_<TASK>/<IMPL>/task_<TASK>_<IMPL>_<SIZE>
```

**Cluster (S3):**
```
s3a://big-data-2026-project/results/task_<TASK>/<IMPL>/task_<TASK>_<IMPL>_<SIZE>
```

Per ispezionare gli output usa gli script in `src/inspect/`:
```bash
python src/inspect/inspect_task_3_1.py
python src/inspect/inspect_task_3_2.py
python src/inspect/inspect_task_3_3.py
```

## Visualizzazione dei risultati

È disponibile uno script per generare automaticamente grafici comparativi dei tempi di esecuzione delle implementazioni Spark Core, Spark SQL e Hive sulle diverse configurazioni di esecuzione.

### Esecuzione

```bash
python plots/generate_plots.py
```
---

## Log

I log di esecuzione registrano tempi, output di Spark/Hive ed eventuali errori, organizzati per task, implementazione e taglia. Nella repository sono già presenti alcuni log di esempio.

**Locale:**
```
logs/spark/<TASK>/<IMPL>/<SIZE>/<TASK>_<IMPL>_<SIZE>_<TIMESTAMP>.log
logs/hive/<TASK>/<SIZE>/<TASK>_<SIZE>_<TIMESTAMP>.log
```

**Cluster (S3):**
```
s3://big-data-2026-project/src/analysis/spark/logs_spark/
```

---

## Upload su AWS S3

Carica i sorgenti del progetto su S3, escludendo automaticamente `venv/`, `logs/`, `metastore_db/`, `derby.log/` e `__pycache__/`.

### Prerequisito — Installare AWS CLI

Se AWS CLI non è ancora installato (Ubuntu/Linux):

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

### Prerequisito — Configurare le credenziali AWS

Le credenziali AWS cambiano a ogni nuova sessione del lab. Prima di eseguire l'upload, aggiornarle con:

```bash
aws configure
```

Inserire quando richiesto: `AWS Access Key ID`, `AWS Secret Access Key`, `AWS Session Token` (se presente) e la regione (`us-east-1` o quella del lab).

### Esecuzione

**Upload di un singolo file:**
```bash
python upload_to_s3/upload_file.py
```

**Upload dell'intero progetto:**
```bash
python upload_to_s3/upload_all.py
```

Il bucket di destinazione è `big-data-2026-project`.
## Spark History Server

Per visualizzare l'interfaccia web con i dettagli delle esecuzioni Spark:

**Locale:**
```bash
$SPARK_HOME/sbin/start-history-server.sh
```
Accessibile su [http://localhost:18080](http://localhost:18080).

**Cluster:** eseguire un tunnel SSH dal proprio terminale locale sostituendo l'indirizzo del nodo master e la propria chiave privata:
```bash
ssh -i your_key.pem -L 18081:localhost:18080 hadoop@<master-node-address>
```
Accessibile su [http://localhost:18081](http://localhost:18081).
## Relazione tecnica

Nella root della repository è disponibile una relazione tecnica che descrive in dettaglio le scelte implementative, i risultati e le considerazioni sul progetto:

```
technical_report.pdf
```

