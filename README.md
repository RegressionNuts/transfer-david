## Quickstart

# 1) Create & activate a virtualenv (recommended)
a. python -m venv .venv
b. .venv\Scripts\activate

# 2) Install dependencies
pip install -r requirements.txt




# data_pipeline
freight cot data
- run cot_eex.py (tuesday)
- run cot_sgx.py (wednesday)
- run cot_email.py right after cot_sgx.py

daily update price
- run everyday when the price is updated in softmar


