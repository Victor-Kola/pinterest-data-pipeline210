import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import datetime


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        url = 'https://fbjbyb6q42.execute-api.us-east-1.amazonaws.com/Test'    
        headers= {'Content-Type': 'application/vnd.kafka.json.v2+json'}

        def serialize_datetime(o):
            if isinstance(o, datetime.datetime) :
                return o.isoformat()
            raise TypeError("Type not serializable")


        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                pin_url ='https://fbjbyb6q42.execute-api.us-east-1.amazonaws.com/Test/topics/126dc60b95b3.pin'
                pin_payload = json.dumps({
    'records': [
        {
            'value': {
                'index': pin_result['index'],
                'unique_id': pin_result['unique_id'],
                'title': pin_result['title'],
                'description': pin_result['description'],
                'poster_name': pin_result['poster_name'],
                'follower_count': pin_result['follower_count'],
                'tag_list': pin_result['tag_list'],
                'is_image_or_video': pin_result['is_image_or_video'],
                'image_src': pin_result['image_src'],
                'downloaded': pin_result['downloaded'],
                'save_location': pin_result['save_location'],
                'category': pin_result['category']
            }
        }
    ]
})

                response = requests.request("POST", pin_url, headers=headers, data=json.dumps(pin_payload))

                print(response.status_code)
                print(f"Response Headers: {response.headers}")
                print(f'Response Content: {response.text}')

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                geo_url = 'https://fbjbyb6q42.execute-api.us-east-1.amazonaws.com/Test/topics/126dc60b95b3.geo'
                geo_payload = json.dumps({
    'records': [
        {
            'value': {
                'ind': geo_result['ind'],
                'timestamp': geo_result['timestamp'].isoformat(),
                'latitude': geo_result['latitude'],
                'longitude': geo_result['longitude'],
                'country': geo_result['country']
            }
        }
    ]
})
                response = requests.request("POST", geo_url, headers=headers, data=json.dumps(geo_payload))
                print(response.status_code)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                user_payload = json.dumps({
    'records': [
        {
            'value': {
                'ind': user_result['ind'],
                'first_name': user_result['first_name'],
                'last_name': user_result['last_name'],
                'age': user_result['age'],
                'date_joined': user_result['date_joined'].isoformat()
            }
        }
    ]
})
                user_url = 'https://fbjbyb6q42.execute-api.us-east-1.amazonaws.com/Test/topics/126dc60b95b3.user'
                response = requests.request("POST", user_url, headers=headers, data=json.dumps(user_payload))
                print(response.status_code)



if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    