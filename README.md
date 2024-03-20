![Workflow](workflow.png)

# main.py:
This script serves as the core functionality for data retrieval, processing, and storage. It interfaces with the openmeteo API to fetch weather data, processes it to derive meaningful insights, and then facilitates the storage of processed data in both CSV and MySQL formats.

# visualisation.py:
The purpose of this script is to create visual representations of the temperature trends over time. It utilises data stored in a CSV file, analyzing the average temperature changes across different years. The visualizations generated provide insights into long-term temperature variations.

# config.yaml:
This YAML configuration file holds essential connection details required for establishing a connection to the MySQL database. Users are instructed to provide their specific destination connection details within this file. These details are utilised by the main.py script during the loading of processed data into the database.

# schema.yaml:
Containing the schema definition for database tables, this YAML script is utilised by the main.py script for creating database tables. It defines the structure of the tables to be created, including column names and data types. Users are expected to customize this schema according to their specific database requirements.

# weather_data_output.csv
Final processed data in CSV
