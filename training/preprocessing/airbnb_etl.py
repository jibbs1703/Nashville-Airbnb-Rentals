import requests
from training.aws_resources.s3 import S3Buckets
from io import StringIO

import pandas as pd


s3_conn = S3Buckets.credentials('us-east-2')


# Function to Extract File URL from AirBnB Website
def get_url(base_url="https://data.insideairbnb.com",
            country="united-states",
            state_code="tn",
            city="nashville",
            data_type=None):
    if data_type is None:
        data_type = ["reviews", "listings"]
    for data in data_type:
        #url = f"{base_url}/{country}/{state_code}/{city}/2024-06-22/data/{data}.csv.gz"  # For csv.gz files
        url = f"{base_url}/{country}/{state_code}/{city}/2024-06-22/visualisations/{data}.csv" # For CSV files
        yield url

def get_data(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    response = requests.get(url, headers=headers)
    name_of_file = url.split('/')[-1]

    if response.status_code == 200:
        data = StringIO(response.text)
        dataframe = pd.read_csv(data)
        return dataframe,name_of_file
    else:
        return f"Cannot get file due to error {response.status_code}"

def write_to_s3(df, bucket_name, filename, folder=''):
    s3_conn.upload_dataframe_to_s3(df=df, bucket_name=bucket_name, object_name=f'{folder}{filename}')

if __name__=="__main__":
    gen = get_url()
    for link in range(2):
        df, filename = get_data(next(gen))
        write_to_s3(df=df, bucket_name='jibbs-machine-learning-bucket',
                    folder='nashville_airbnb/', filename=filename)

