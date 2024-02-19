# dataplatform

This project pulls open source NYC taxi data, processes it using dbt and duckdb, and TBD how we will display that information.

It's all orchestrated with Dagster. The BI tool of choice will be either Streamlit, Lightdash, or Metabase.

# Setup - Windows

##### 1. Clone the repo
##### 2. Install dependencies with either pipenv or requirements.txt
##### 3. Pipenv:
run `pip install pipenv`
run `pipenv install` at the repo root. this will install all the dependencies in the pipfile
run `pipenv shell` to enter the virtual environment
##### 4. Using requirements.txt
run `python -m venv ./.venv/` at the repo root. This sets up a virtual environment
run `./.venv/Scripts/activate` to use the venv
run `pip install -r requirements.txt` to install all dependencies
##### 5. Create two folders at the root: "duckdb" and "data_lake"
##### 6. cd into dagster/nyctaxi
##### 7. Run dagster dev
##### 8. Open localhost:3000 in your browser

# Setup - Mac

##### 1. Clone the repo
##### 2. Install dependencies with requirements.txt
run `python -m venv ./.venv/` (or `python3 -m venv ./.venv/`) at the repo root. This sets up a virtual environment. 
run `source ./.venv/bin/activate` to enter the venv.
run `pip install -r requirements.txt` to install all dependencies.
##### 3. Create two folders at the root: "duckdb" and "data_lake"
##### 4. cd into dagster/nyctaxi
##### 5 Run dagster dev
##### 6. Open localhost:3000 in your browser

# Notes on setting up dbt in dagster
The constants.py file defines where the project yml file is for the dbt project. It also has logic to create the manifest.json file which dagster uses to integrate the dbt models. 

You need to define an env var "DAGSTER_DBT_PARSE_PROJECT_ON_LOAD" = 1. You can do so in the .env file. On Mac, you can run ‘nano ~/.zshrc’ and add the line “export DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1” to the end, then restart your terminal.

To connect your non-dbt assets to dbt models in the dag, you add the metadata info in the sources.yml file. 

# Data visualization 

Currently supports cursory visualization of small (< 50 MB) datasets using streamlit. To use, run "streamlit run streamlit.py" in the data_viz_tools directory.



