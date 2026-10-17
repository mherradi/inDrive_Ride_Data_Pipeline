# Data Maroc Boot Camps Lab 3 — inDrive Ride Data ELT Pipeline Pipeline
&nbsp;&nbsp;&nbsp;This project integrates hybrid data ingestion from a simulated inDrive real-time streaming via Mockaroo REST API + batch dimension ingestion via local CSVs into PostgreSQL 
using Apache Airflow, modeled across a multi-tier Architecture.
&nbsp;
&nbsp;

##  Tools and Platforms :
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker_Compose-2496ED?style=flat&logo=docker&logoColor=white)
![GNU AWK](https://img.shields.io/badge/GNU_AWK-555555?style=flat&logo=gnu&logoColor=white)
![Bash](https://img.shields.io/badge/Bash-4EAA25?style=flat&logo=gnubash&logoColor=white)
![Linux](https://img.shields.io/badge/Linux-FCC624?style=flat&logo=linux&logoColor=black)
![ETL Pipeline](https://img.shields.io/badge/ETL-Pipeline-2E8B57?style=flat)
![CSV Processing](https://img.shields.io/badge/CSV-Processing-00897B?style=flat)
![Automation](https://img.shields.io/badge/Processing-Automated-orange?style=flat)
&nbsp;
## Architecture Overview
This project models a simple ELT architecture (Extract, Load, Transform) workflow:
<img width="1672" height="941" alt="Pipeline de données de trajets inDrive" src="https://github.com/user-attachments/assets/11e5c1a5-c347-4694-857b-ae8a62ab182c" />
&nbsp;
## Project Files :
```text
indrive-ride-pipeline/
├── dags/
│   ├── streaming_dag.py            # Micro-batch DAG polling Mockaroo API every minute
│   ├── batch_dag.py                # Batch DAG loading drivers, vehicles, and zones CSVs
│   ├── marts_dag.py                # ELT DAG executing staging and mart SQL scripts
│   ├── staging_dag.py
│   └── db.py      
├── data/                           # Local batch dimension files
│       ├── drivers.csv
│       ├── vehicles.csv
│       └── zones.csv
├── postgres/                           # Local batch dimension files
│       ├── init_db.sql               # Database schema initialization
├── sql/
│   ├── staging/
│   │   ├── drivers.sql           # Cleans text, drops phone PII
│   │   ├── riders.sql           # Cleans text, drops phone PII
│   │   ├── vehicles.sql           # Cleans text, drops phone PII
│   │   └── zones.sql             # Deduplicates, drops orphan drivers and non-positive fares
│   └── mart/
│       ├── rides.sql                 # Enriched ride fact table with Role-Playing zones
│       ├── revenue_by_zones.sql      # Sum the total revenue for each zone
│       └── driver_leaderboard.sql    # Number of rides and total revenue for each driver
├── docker-compose.yaml
└── README.md
```
&nbsp;
## Airflow DAG's
In the following a screenshot of each Airflow DAG in a green State.
<img width="1867" height="607" alt="Screenshot 01" src="https://github.com/user-attachments/assets/d826b19e-10e9-4150-9b9e-bc53d9083449" />
 &nbsp;
 <img width="1877" height="502" alt="Screenshot 02" src="https://github.com/user-attachments/assets/736de572-dd77-45e9-8f9d-6dd3e7885120" />
 &nbsp;
<img width="1885" height="612" alt="Screenshot 03" src="https://github.com/user-attachments/assets/bd76b68e-9fe0-4c02-a80e-6b51c98d9f4f" />
 &nbsp;
<img width="1887" height="606" alt="Screenshot 04" src="https://github.com/user-attachments/assets/e3a43cb3-ddc1-4edc-af4d-4c380d5959cd" />
 &nbsp;
<img width="1880" height="512" alt="Screenshot 05" src="https://github.com/user-attachments/assets/554361d2-4308-4386-ad87-643f59379858" />
 &nbsp;

## Cleanup
```bash
docker compose down        # stop (data is kept in the pgdata and airflow_home volumes)
docker compose down -v     # stop and delete the volumes (full reset)
```

Postgres data and Airflow history are stored in Docker volumes, so they survive restarts. `postgres/init.sql` only runs on an empty volume: after editing it, do a full reset with `down -v`.

## Troubleshooting
| Problem | Fix |
|---|---|
| Airflow page not loading | wait 1 min, then `docker compose logs airflow` |
| DAGs not visible yet | wait 30 s and refresh, the scheduler scans the folder regularly |
| `3_staging` run is skipped (pink) | `1_streaming` or `2_batch` has not succeeded yet: trigger it, the next staging run goes through |
| `CERTIFICATE_VERIFY_FAILED` | set `VERIFY_SSL` to `"false"` (step 2) |
| Port already in use | change the left number in `"8081:8080"` / `"8085:8080"` in `docker-compose.yml` |
| Adminer cannot connect | Server must be `postgres` (not `localhost`) |
| Git Bash turns `/opt/...` into a Windows path | prefix the command with `MSYS_NO_PATHCONV=1` |


