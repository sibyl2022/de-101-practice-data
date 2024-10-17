import shutil
import zipfile
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import logging
import requests
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, year

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, os.pardir))
data_dir = os.path.join(project_root, "data")
extracted_dir = os.path.join(project_root, "extracted_data")

base_url = "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_{}.zip"
file_name_pattern = r"data_Q\d{1,}_\d{4}"
source_folder_pattern = r"Q\d{1}_\d{4}"


def rename_file(new_name, path):
    for file in os.listdir(path):
        if os.path.isdir(os.path.join(path, file)):
            sub_dir = os.path.join(path, file)
            for item in os.listdir(sub_dir):
                if item.endswith(".csv"):
                    os.rename(os.path.join(sub_dir, item), os.path.join(path, new_name))


def remove_file(path):
    for file in os.listdir(path):
        source_dir = os.path.join(path, file)
        if os.path.isdir(source_dir):
            shutil.rmtree(source_dir)


def download_file(url, file_path):
    print(file_path)
    if os.path.exists(file_path):
        print(f"File already exists: {file_path}")
        return True

    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024

    with open(file_path, 'wb') as file:
        downloaded_size = 0
        for data in response.iter_content(block_size):
            file.write(data)

    if total_size != 0 and downloaded_size != total_size:
        print('Failed to download completely: {}'.format(file_path))
        os.remove(file_path)
        return False

    print(f"Downloaded: {file_path}")
    return True


def download_files_from_backblaze(start_year, end_year, end_quarter=4):
    for year_value in range(start_year, end_year + 1):
        final_quarter = end_quarter if year_value == end_year else 4
        for quarter in range(1, final_quarter + 1):
            download_url = base_url.format(f"Q{quarter}_{year_value}")
            file_name = f"Q{quarter}_{year_value}.zip"
            download_file_path = os.path.join(data_dir, file_name)
            success = download_file(download_url, download_file_path)
            if not success:
                error_msg = f"Download Backblaze data for {year_value} Q{quarter} failed."
                logging.error(error_msg)
                raise Exception(error_msg)


def unzip_files_from_backblaze():
    for root, _, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".zip"):
                zip_file_path = os.path.join(root, file)
                extract_dir = os.path.join(os.path.dirname(root), "extracted_data", os.path.splitext(file)[0])

                if os.path.exists(extract_dir):
                    print(f"Skipping extraction for {zip_file_path}. Directory already exists.")
                    continue
                try:
                    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_dir)

                        extracted_files = os.listdir(extract_dir)
                        for extracted_file in extracted_files:
                            if re.match(file_name_pattern, extracted_file) and os.path.isdir(
                                    os.path.join(extract_dir, extracted_file)):
                                sub_dir = os.path.join(extract_dir, extracted_file)
                                for item in os.listdir(sub_dir):
                                    shutil.move(os.path.join(sub_dir, item), extract_dir)
                                os.rmdir(sub_dir)

                        macosx_dir = os.path.join(extract_dir, '__MACOSX')
                        if os.path.exists(macosx_dir):
                            print(f"Removing {macosx_dir}")
                            shutil.rmtree(macosx_dir)
                except zipfile.BadZipFile:
                    logging.error(f"Error: {zip_file_path} is not a valid ZIP file. Skipping extraction.")
                    continue


def generate_daily_summary(file_path, file_name):
    spark = SparkSession.builder.appName("dailySummary").getOrCreate()

    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        df = df.withColumn("brand",
                           when(col("model").startswith("CT"), "Crucial")
                           .when(col("model").startswith("DELLBOSS"), "Dell BOSS")
                           .when(col("model").startswith("HGST"), "HGST")
                           .when(col("model").startswith("Seagate") | col("model").startswith("ST"), "Seagate")
                           .when(col("model").startswith("TOSHIBA"), "Toshiba")
                           .when(col("model").startswith("WDC"), "Western Digital")
                           .otherwise("Others"))

        model_summary = df.groupBy("brand").agg({"brand": "count", "failure": "sum"}) \
            .withColumnRenamed("count(brand)", "drive_count") \
            .withColumnRenamed("sum(failure)", "drive_failures")

        output_dir = os.path.join(project_root, "daily_output")
        output_file = os.path.join(output_dir, f"{file_name}")

        model_summary.write.csv(output_file, header=True, mode="overwrite")

        new_name = f"{file_name}.csv"
        rename_file(new_name, output_dir)
        remove_file(output_dir)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        spark.stop()


def process_data_by_date():
    for root, _, files in os.walk(extracted_dir):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                file_name = file.split('.')[0]
                try:
                    generate_daily_summary(file_path, file_name)
                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {str(e)}")


def check_data_complexity():
    all_quarters = {}
    for file in os.listdir(extracted_dir):
        if re.match(source_folder_pattern, file):
            year_value = file.split("_")[1]
            quarter = file.split("_")[0]
            if year_value not in all_quarters:
                all_quarters[year_value] = set()
            all_quarters[year_value].add(quarter)

    missing_quarters = []
    missing_years = []
    for year_value, quarters in all_quarters.items():
        if len(quarters) != 4:
            missing_quarters.append((year_value, [f"Q{i}" for i in range(1, 5) if f"Q{i}" not in quarters]))

    if missing_quarters:
        for year_value, missing in missing_quarters:
            logging.error(f"Year {year_value}: Missing quarters {', '.join(missing)}")
            missing_years.append(year_value)

    return missing_years


def check_yearly_data_complexity(**context):
    missing_years = check_data_complexity()
    context['ti'].xcom_push(key='missing_years', value=missing_years)


def generate_yearly_summary(missing_years):
    file_path = []
    year_list = set()
    for file in os.listdir(extracted_dir):
        data_year = file.split("_")[1]
        if os.path.isdir(os.path.join(extracted_dir, file)) and data_year not in missing_years:
            year_list.add(data_year)
            sub_dir = os.path.join(extracted_dir, file)
            for item in os.listdir(sub_dir):
                file_path.append(os.path.join(sub_dir, item))

    spark = SparkSession.builder.appName("yearlySummary").getOrCreate()

    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        df = df.withColumn("Date", to_date(col("Date")))

        df = df.withColumn("brand",
                           when(col("model").startswith("CT"), "Crucial")
                           .when(col("model").startswith("DELLBOSS"), "Dell BOSS")
                           .when(col("model").startswith("HGST"), "HGST")
                           .when(col("model").startswith("Seagate") | col("model").startswith("ST"), "Seagate")
                           .when(col("model").startswith("TOSHIBA"), "Toshiba")
                           .when(col("model").startswith("WDC"), "Western Digital")
                           .otherwise("Others"))

        for year_item in year_list:
            df_year = df.filter((year(col("Date")) == year_item))

            brand_failure_count = df_year.groupBy("brand").agg({"failure": "sum"}) \
                .withColumnRenamed("sum(failure)", "drive_failures") \
                .withColumn("drive_failures", col("drive_failures").cast("integer"))

            output_dir = os.path.join(project_root, "yearly_output")
            output_file = os.path.join(output_dir, f"{year_item}")

            brand_failure_count.write.csv(output_file, header=True, mode="overwrite")

            new_name = f"{year_item}.csv"
            rename_file(new_name, output_dir)
            remove_file(output_dir)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        spark.stop()


def process_data_by_year(**context):
    missing_years = context['ti'].xcom_pull(key='missing_years', task_ids='check_yearly_data_complexity_task')
    generate_yearly_summary(missing_years)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'data_processing_dag',
    default_args=default_args,
    description='A DAG to download data, process it, and output results',
    schedule_interval=None,
)

# Download data from Backblaze website
# Data range: 2019 Q1 - 2023 Q3
# ImportError: urllib3 v2.0 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with LibreSSL 2.8.3
# pip install urllib3==1.26.15
download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_files_from_backblaze,
    op_kwargs={'start_year': 2019, 'end_year': 2023, 'end_quarter': 3},
    dag=dag,
)

unzip_task = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_files_from_backblaze,
    dag=dag,
)

daily_summary_task = PythonOperator(
    task_id='daily_summary_task',
    python_callable=process_data_by_date,
    dag=dag,
)

check_yearly_data_complexity_task = PythonOperator(
    task_id='check_yearly_data_complexity_task',
    python_callable=check_yearly_data_complexity,
    provide_context=True,
    dag=dag,
)

yearly_summary_task = PythonOperator(
    task_id='yearly_summary_task',
    python_callable=process_data_by_year,
    provide_context=True,
    dag=dag,
)

download_task >> [unzip_task]

unzip_task >> [daily_summary_task, check_yearly_data_complexity_task]

check_yearly_data_complexity_task >> yearly_summary_task
