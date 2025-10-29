# 📊 Project P2: BI Python – Reading Raw Data into pandas DataFrames

> Use this repo to start a professional Python BI project that reads raw data into pandas DataFrames.

- Course: Business Intelligence (Pro Analytics 02)
- Instructor: Dr. Denise Case
- Repository Template: <https://github.com/denisecase/pro-analytics-02>
- Author: **Sabri Hamdaoui**, MBA – Data Analytics
- University: Northwest Missouri State University
- GitHub: [sabrouch36](https://github.com/sabrouch36)

---

## 🎯 Project Objective
This project demonstrates how to read raw business data (CSV files) into **pandas DataFrames** using Python, as part of a Business Intelligence (BI) data pipeline.
It emphasizes reusable logic, structured code, logging, and Git-based project management.

---

## 🧠 Key Learning Outcomes
- Automate the reading of CSV files into pandas DataFrames.
- Use **loguru** for professional, centralized logging.
- Structure a Python project using the `src/analytics_project/` folder.
- Apply **Git version control** for collaborative data projects.
- Understand BI workflows that prepare data for analysis and visualization.

---

## 🧩 Project Structure
smart-store-sabri/
│
├── data/
│ └── raw/
│ ├── customers_data.csv
│ ├── products_data.csv
│ └── sales_data.csv
│
├── src/
│ └── analytics_project/
│ ├── data_prep.py
│ └── utils_logger.py
│
├── project.log
├── pyproject.toml
└── README.md

---

## ⚙️ Setup & Execution Workflow

### 1️⃣ Environment Setup
Create and activate your local environment.

```bash
uv venv
uv python pin 3.12
uv sync --extra dev --extra docs --upgrade
uv run pre-commit install
uv run python --version
Windows (PowerShell)
.\.venv\Scripts\activate

3️⃣ Example Log Output
2025-10-28 22:38:INFO AT utils_logger.py:105: Logging to file: C:\Users\sabri\projects\smart-store-sabri\project.log
2025-10-28 22:38:INFO AT data_prep.py:52: Starting data preparation...
2025-10-28 22:38:INFO AT data_prep.py:35: Reading raw data from customers_data.csv
2025-10-28 22:38:INFO AT data_prep.py:38: customers_data.csv: loaded DataFrame with shape 201 rows x 4 cols
2025-10-28 22:38:INFO AT data_prep.py:35: Reading raw data from products_data.csv
2025-10-28 22:38:INFO AT data_prep.py:38: products_data.csv: loaded DataFrame with shape 100 rows x 4 cols
2025-10-28 22:38:INFO AT data_prep.py:35: Reading raw data from sales_data.csv
2025-10-28 22:38:INFO AT data_prep.py:38: sales_data.csv: loaded DataFrame with shape 2001 rows x 7 cols
2025-10-28 22:38:INFO AT data_prep.py:64: Data preparation complete.

4️⃣ Git Version Control Commands

Use Git frequently to track progress and sync your work to GitHub.
git add .
git commit -m "Add data_prep and utils_logger modules"
git push -u origin main

Later updates:
git add README.md
git commit -m "Update README with workflow, results, and reflection"
git push

5️⃣ Results Summary

All raw data files were successfully loaded into DataFrames.

File Name	Rows	Columns
customers_data.csv	201	4
products_data.csv	100	4
sales_data.csv	2001	7

✅ Logs confirm that the ETL (Extract–Transform–Load) pipeline executed without errors.

🧠 Reflection

Through this project, I learned to:

Implement modular BI data pipelines in Python.

Use logging for debugging and traceability.

Manage code versions professionally with Git and GitHub.

Prepare data for future analytics or visualization workflows.

This experience builds strong foundational skills for real-world business intelligence and data analytics work.

🧰 Optional Development Tools & Checks

To maintain professional project standards:

uv sync --extra dev --extra docs --upgrade
uv cache clean
git add .
uvx ruff check --fix
uvx pre-commit autoupdate
uv run pre-commit run --all-files
uv run pytest


Run documentation build (optional for BI reporting):

uv run mkdocs build --strict
uv run mkdocs serve

👨‍💻 Author

Sabri Hamdaoui
MBA – Data Analytics | Northwest Missouri State University
GitHub: sabrouch36

✅ Final Verification Checklist

 data_prep.py reads all CSV files correctly

 Logs appear in project.log

 utils_logger.py configured successfully

 README updated with workflow, outputs, and reflection

 Project pushed to GitHub
