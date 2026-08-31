# Real-Time Flight & Airport Operations Analytics — Cloud Deployment

This version needs **no Docker and nothing running on your own laptop**.
Three free services do the work:

| Piece | Service | Cost |
|---|---|---|
| Database | Aiven (free MySQL, always-on) | $0 |
| Ingestion + ETL | GitHub Actions (scheduled jobs) | $0 |
| Dashboard | Render (free Web Service) | $0 |

## Honest trade-offs vs. the Docker version

- Ingestion/ETL run **on a schedule** (every 10-15 min), not a continuous
  loop. You can also trigger them manually for an instant refresh.
- GitHub's cron scheduling can occasionally run a few minutes late under
  load — for a guaranteed-instant update (e.g. right before a demo), use
  the manual "Run workflow" button instead of waiting on the schedule.
- Render's free tier sleeps after 15 minutes with no visitors — the
  first visit after sleeping takes ~30-60s to wake up. Open your
  dashboard link a minute or two before you actually need it.
- Aiven's free MySQL auto-powers-off after a long period of total
  inactivity (you'd get an email first). Regular scheduled ingestion
  runs naturally prevent this in normal use.

## One-time setup

### 1. Create the free Aiven MySQL database
1. Sign up at https://aiven.io (no credit card needed)
2. Create a new service -> MySQL -> Free plan
3. Once it's running, go to the service's **Overview** tab and note:
   `Host`, `Port`, `User` (default `avnadmin`), `Password`, and default
   database name (or create one called `flight_ops_analytics`)

### 2. Create the schema
Connect to your Aiven MySQL using any MySQL client (MySQL Workbench,
TablePlus, DBeaver, or the `mysql` CLI) with the credentials from step 1,
then run the contents of `database/init/01_schema.sql` once.

### 3. Push this project to GitHub
Create a new GitHub repository and push this folder to it.

### 4. Add GitHub Secrets
In your repo: **Settings -> Secrets and variables -> Actions -> New
repository secret**. Add each of these:

| Secret name | Value |
|---|---|
| `FLIGHT_DB_HOST` | your Aiven host |
| `FLIGHT_DB_USER` | your Aiven user |
| `FLIGHT_DB_PASSWORD` | your Aiven password |
| `FLIGHT_DB_NAME` | `flight_ops_analytics` |
| `OPENSKY_CLIENT_ID` | (optional but recommended — see below) |
| `OPENSKY_CLIENT_SECRET` | (optional but recommended) |

### 5. Seed airport reference data (one click, one time)
Go to your repo's **Actions** tab -> **Seed Airport Reference Data** ->
**Run workflow**. Takes under a minute; loads ~7,700 airports.

### 6. Confirm the scheduled workflows are enabled
Still in the **Actions** tab, you should see **Ingest OpenSky Data** and
**Run ETL** listed. GitHub sometimes disables scheduled workflows on
new repos until you visit the Actions tab once — just opening it is
enough to activate them.

### 7. Deploy the dashboard to Render
1. Sign up at https://render.com (connects directly to GitHub)
2. New -> Web Service -> select this repository
3. Runtime: Python 3
4. Build command: `pip install -r requirements.txt`
5. Start command: `streamlit run dashboard/dashboard.py --server.port=$PORT --server.address=0.0.0.0`
6. Add environment variables (Render's dashboard, not GitHub Secrets):
   `FLIGHT_DB_HOST`, `FLIGHT_DB_USER`, `FLIGHT_DB_PASSWORD`, `FLIGHT_DB_NAME`
   — same values as your Aiven credentials
7. Deploy. Render gives you a public URL like
   `https://your-app.onrender.com` — that's the link you give your teacher.

## Before your demo

1. Go to Actions -> **Ingest OpenSky Data** -> Run workflow (manual, instant)
2. Go to Actions -> **Run ETL** -> Run workflow (manual, instant)
3. Open your Render dashboard URL a minute or two early, so it's already
   awake by the time you present
4. During the demo, you can re-trigger step 1 and refresh the dashboard
   to visibly show new data arriving

## Getting OpenSky API credentials (recommended)

Anonymous access works but is capped at 400 credits/day. For a free
account with a higher limit: sign up at opensky-network.org -> Account
-> API Client -> create a client, then add the ID/secret as GitHub
Secrets (step 4 above).

## A note on Aiven and SSL

Aiven's MySQL requires an encrypted connection. The connector used here
(`mysql-connector-python`) attempts SSL automatically when the server
supports it, so this should work without extra configuration. If you
hit an SSL-related connection error, download the CA certificate from
your Aiven service's Overview page and add `ssl_ca=<path>` to the
`DB_CONFIG` dictionaries in `ingestion/ingest_opensky.py`,
`etl/build_gold_tables.py`, `etl/pipeline_worker.py`, and
`dashboard/dashboard.py`.
