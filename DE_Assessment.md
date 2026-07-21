
**Technical Assessment**

Data Engineer

| Role | Data Engineer — Junior (0–2 years) |
| :---- | :---- |
| **Format** | Coding challenge \+ design question |
| **Environment** | Local — Docker Compose (provided) |
| **Est. Time** | 3 days |
| **Submission** | Git repository (zip or link) |

# **Overview**

Thank you for your interest in the Data Engineer role. This assessment is designed to evaluate your ability to build reliable data pipelines, model data appropriately, and reason about system design.

You will work with a realistic event-level dataset and a pre-configured local environment. We are not looking for a perfect production system — we are looking for clean thinking, working code, and honest trade-off reasoning.

# **Environment**

A docker-compose.yml file is provided alongside this document. It starts the services you will need:

| postgres | Target database — your pipeline writes here |
| :---- | :---- |
| **airflow** | Orchestration — accessible at http://localhost:8080 |
| **kafka** | Message broker for Part C |
| **zookeeper** | Required by Kafka |

To start the environment:

| docker compose up \-d \# Airflow UI: http://localhost:8080  (user: airflow / pass: airflow) \# Postgres:   localhost:5432         (db: assessment / user: de / pass: de) |
| :---- |

You do not need to modify the Docker Compose file. All work should live in your own project files mounted into the containers.

# **Dataset**

**You will work with: Event Records — Q4 2024**

The file de\_assessment\_data.csv contains event-level records. Each row represents one discrete event with a timestamp, identifiers, categorical attributes, and numeric values.

File provided: de\_assessment\_data.csv  (\~300,000 rows)

## **Columns**

| event\_id | Unique identifier for each event |
| :---- | :---- |
| **event\_timestamp** | When the event occurred (datetime string, UTC) |
| **entity\_id** | Identifier of the entity associated with the event |
| **zone\_id** | Zone or location category |
| **event\_type** | Category of event |
| **duration** | Duration of the event in seconds |
| **value** | Numeric outcome associated with the event |
| **sub\_value** | Secondary numeric value |
| **total\_value** | Sum of value \+ sub\_value \+ fees |
| **payment\_method** | Payment category |
| **passenger\_count** | Count of participants |
| **vendor\_id** | Source system identifier |

# **Tasks**

The assessment has three technical parts and one design question. All parts use the same dataset.

| PART A — Data Modeling & SQL |
| :---- |

Design a relational schema to store the event data in PostgreSQL. Your schema should be appropriate for analytical queries — not just a raw dump of the CSV.

| Create your schema using SQL DDL (CREATE TABLE statements). Apply appropriate data types, primary keys, and indexes. Write a SQL script that loads data from the CSV into your schema, performing any necessary transformations. Write two analytical queries against your schema of your choosing. Include a brief comment explaining what each query answers and why you found it worth asking. |
| :---- |

Deliverable: schema.sql, load.sql, queries.sql

| PART B — Pipeline & Orchestration |
| :---- |

Build an Airflow DAG that automates the ingestion and transformation from Part A.

| The DAG should: (1) ingest the CSV into a raw staging table, (2) apply your transformations to populate the analytical schema, (3) run on a daily schedule. Handle failures gracefully — the pipeline should be safe to re-run without duplicating data. Add basic logging so it is clear what the pipeline did and whether it succeeded. |
| :---- |

Deliverable: dags/pipeline.py (and any supporting modules)

| PART C — Streaming Ingestion |
| :---- |

Simulate a real-time event stream and build a consumer that processes it.

| Write a producer script that reads rows from de\_assessment\_data.csv and publishes them to a Kafka topic (events) one by one, with a short delay between messages. Write a consumer script that reads from the topic and writes processed records into PostgreSQL in near-real-time. You define what 'processed' means — at minimum, parse and insert; optionally, aggregate or enrich. Both scripts should run as standalone Python processes (not inside Airflow). |
| :---- |

Deliverable: streaming/producer.py, streaming/consumer.py

| PART D — Design Question |
| :---- |

Answer the following question in writing (max 1 page):

| The data volume grows 100x and events must be available for querying within 30 seconds of occurring. What would you change in your current architecture? What would you keep? What are the main trade-offs in your proposed approach? |
| :---- |

There is no single correct answer. We are interested in your reasoning and awareness of trade-offs.

# **Deliverables**

Submit a single zipped folder or Git repository with the following structure:

| assessment/ ├── dags/ │   └── pipeline.py ├── streaming/ │   ├── producer.py │   └── consumer.py ├── sql/ │   ├── schema.sql │   ├── load.sql │   └── queries.sql ├── design.md          ← Part D written answer ├── requirements.txt   ← Python dependencies └── README.md          ← Setup & run instructions |
| :---- |

**The README is important — it should tell us how to run your solution from scratch. Assume we will follow it exactly.**

# **Evaluation Criteria**

Submissions will be reviewed across four dimensions:

| Dimension | What We Look For | Weight |
| :---- | :---- | :---- |
| Correctness | Does the pipeline produce accurate, consistent output? | 35% |
| Code Quality | Readable, modular, documented code; sensible project structure | 25% |
| Reliability | Error handling, idempotency, basic logging | 20% |
| Design Thinking | Schema choices, scalability awareness, written reasoning | 20% |

A pipeline that runs and produces correct output with a clear README will score higher than a sophisticated but broken one.

# **Notes**

* You may use any Python libraries. List them in requirements.txt.

* SQL should target PostgreSQL 15 syntax.

* If you run out of time, submit what you have and note in the README what remains and how you would approach it.

* If you make design decisions that deviate from the instructions, explain why in the README.

* Do not include the data file in your submission.

