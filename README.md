# airquality-data-pipeline
A Data pipeline use case that using OpenAQ Dataset

### What is OpenAQ?
OpenAQ is a free, open-source, community-driven database of air quality data.

### What is the purpose of this project?
This project is to use OpenAQ dataset to build a data pipeline that can be used to predict air quality in a city.

### Dataset
- The dataset is available at [OpenAQ](https://openaq.org/datasets).
- For dataset exploration -> [Jupyter Notebook](https://github.com/kivancgunduz/airquality-data-pipeline/blob/master/guide/data_exploring.ipynb) 



### Project Description
- Get the data from OpenAQ with [SingerETL](https://www.singer.io/)
    - SingerETL is a data pipeline tool that can be used to extract, transform, load, and analyze data.
- Transform the data with [Apache Spark](https://spark.apache.org/)
    - Apache Spark is a fast, scalable, and fault-tolerant framework for processing large datasets.
- Load the data into a database with [Sqlite3](https://www.sqlite.org/)
    - Sqlite3 is a relational database management system.

### Usage
- Clone the project
- Install the dependencies with [`pip install -r requirements.txt`]
- Run the tap with [`python ./ingest/openaq_tap/tap_airquality_sheets/tap_airquality_sheets.py | target-csv --config ./ingest/data_lake.conf`]
- Run the pipeline with [`python ./spark_pipelines/airquality/cleansing/preprocessing.py`]

### Database Information
- Technology: [Sqlite3](https://www.sqlite.org/)
- Database name: test_database
- Table name: airquality
- Columns:
    - country: string
    - city: string
    - parameter: string
    - value: float
    - aqi_pm25: float
    - aqi_pm10: float

<img src='https://i0.wp.com/semantix.com.br/wp-content/uploads/2021/06/smtx-data-platform-1.gif'>
