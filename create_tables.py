from google.cloud import bigquery

client = bigquery.Client(project='bachelor-data-analysis')

dataset_id = 'bachelor_data'
table_files = {
    'contestant_details': 'contestant_details.csv',
    'event_log': 'eliminations_and_dates.csv',
    'hometown_coordinates': 'hometown_coordinates.csv',
    'season_details': 'season_details.csv',
}

dataset_ref = client.dataset(dataset_id)
dataset = client.create_dataset(bigquery.Dataset(dataset_ref))
print('Created dataset {}.'.format(dataset_id))

for table_id, file_name in table_files.items():
    table_ref = dataset.table(table_id)

    with open(file_name, 'rb') as file:
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV
        job = client.load_table_from_file(
            file, table_ref, job_config=job_config)

    job.result()  # Waits for job to complete
    print('Created table {}.'.format(table_id))