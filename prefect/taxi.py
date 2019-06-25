import requests
import pandas as pd
from prefect import task, Flow


@task
def download_file(filename):
    url = f'https://s3.amazonaws.com/nyc-tlc/trip+data/{filename}'
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(filename, 'w') as f:
        for chunk in response.iter_lines():
            f.write('{}\n'.format(chunk.decode('utf-8')))
    return filename


@task
def calculate_data(filename):
    with open(filename) as f:
        df = pd.read_csv(f)
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['pickup_date'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d')
        df = df.groupby('pickup_date')['tip_amount', 'total_amount'].sum().reset_index()
        print(df.head(15))


with Flow("My First Flow") as flow:
    fname = download_file('yellow_tripdata_2018-12.csv')
    calculate_data(fname)


flow.run()
