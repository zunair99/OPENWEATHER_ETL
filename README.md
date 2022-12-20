# OpenmapWeather API Data Pipeline

## This project repository uses the OpenmapWeather API along with the World Cities CSV from the simplemaps database to create an ETL using Airflow, Pandas and Postgres.

### Data is extracted from the OpenmapWeather API on a daily basis from 100 cities based on their latitude and longitude. 
### Extracted data is joined to the World Cities CSV to map the lat and lng points to their respective cities and countries.
### Joined data is stored in a Postgres database.
### Airflow is used to orchestrate and schedule these tasks to run daily. Data is added to the Postgres data warehouse on the frequency of the Airflow tasks.
### Airflow and Postgres are ran in a Docker container for compartmentalization.# OpenWeatherMap-ETL

![logo_white_cropped](https://user-images.githubusercontent.com/75954323/208560365-558b341e-f294-4bd9-8b8b-db336ad0ff99.png)
