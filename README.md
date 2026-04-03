\# 🇬🇧 UK Data \& AI Job Market Intelligence Pipeline



An end-to-end data engineering pipeline that scrapes, processes and visualises UK data and AI engineering job postings daily — with built-in visa sponsorship tracking using the official UK Home Office register.



\## 🔗 Live Dashboard

\[View Live →](YOUR\_AWS\_URL\_HERE)



\## 📌 What It Does

\- Scrapes data and AI engineering jobs daily from Reed.co.uk via their official API

\- Uses OpenAI GPT-4o-mini to extract structured data from raw job descriptions: skills, salary, seniority, work mode, contract type and visa sponsorship mentions

\- Cross-references every employer against the UK Home Office Register of Licensed Sponsors (140,000+ companies)

\- Transforms raw data into analytics-ready models using dbt

\- Displays everything in a live Streamlit dashboard with filters for sponsorship, seniority, work mode and contract type



\## 🏗️ Architecture

Reed API → PostgreSQL (raw layer) → OpenAI Extraction → Home Office Sponsor Register → dbt Transformations → Streamlit Dashboard → AWS EC2



\## 🛠️ Tech Stack

| Tool | Purpose |

|------|---------|

| Python | Core language |

| Requests | Data ingestion from Reed API |

| PostgreSQL | Data storage (bronze and silver layers) |

| OpenAI GPT-4o-mini | Structured data extraction from job descriptions |

| dbt | SQL transformations and analytics models |

| Apache Airflow | Daily pipeline orchestration |

| Streamlit | Interactive dashboard |

| Docker | Local containerisation |

| AWS EC2 + RDS | Cloud deployment |



\## 📊 Dashboard Features

\- Skills Demand — most in-demand technical skills across live job postings

\- Salary Intelligence — average salary bands by seniority and location

\- Sponsoring Companies — jobs from companies verified on the Home Office register

\- Find and Apply — filter by seniority, work mode, contract type and sponsorship status with direct apply links



\## 🚀 Run Locally

1\. Clone the repo

2\. Run: docker-compose up -d

3\. Run: python init\_db.py

4\. Run: python scraper/reed\_scraper.py

5\. Run: python extractor.py

6\. Run: python sponsor\_register.py

7\. Run: python run\_models.py

8\. Run: streamlit run dashboard/app.py



\## 📁 Project Structure

\- scraper/ — Reed API scraper

\- dashboard/ — Streamlit dashboard

\- data/ — Database schema

\- job\_market/ — dbt transformation models

\- extractor.py — OpenAI extraction pipeline

\- sponsor\_register.py — Home Office register integration

\- run\_models.py — dbt model runner

\- db.py — Database helper

\- docker-compose.yml — PostgreSQL container

