# Schiphol API ETL
The files `schiphol_pipeline.py` and `schiphol_pipeline.ipynb` contain the same code for the pipeline. The data is extracted from the API source and inserted or updated into the database. This program could be ran periodically for continuous updates. The data model for the MySQL database is included in `schiphol data model.png` and `schiphol data model.mwb` for use with MySQL Workbench. With the credentials from `config.ini` a connection to the database can be made. Under normal circumstances this file would not be uploaded to a repository. 
