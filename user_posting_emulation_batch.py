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
# The class to connect to the AWS database. 
    def __init__(self):
        # Initialising the connection parameters.
        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        # Method to create and return a database engine connection,
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector() # Creating an instance of the connection.


def run_infinite_post_data_loop():
    # Function to run an infinite loop to post data periodically. 
    while True: #Starting the infinite loop.
        sleep(random.randrange(0, 2)) # Pausing the execution for a random time between 0 and 2 seconds.
        random_row = random.randint(0, 11000) # Selecting a random row number.
        engine = new_connector.create_db_connector()  # Creating a new connection to the database.
        headers= {'Content-Type': 'application/vnd.kafka.json.v2+json'} # Header for the HTTP requests

        def serialize_datetime(o):
            # Function to serialise the datetime objects into ISO format.
            if isinstance(o, datetime.datetime) :
                return o.isoformat() # Returning the ISO format of the datetime objct.
            raise TypeError("Type not serializable")


        with engine.connect() as connection:
            # Establishing a connection with the database.
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            # Query to select a single row from the 'pinterest_data' table randomly. 
            pin_selected_row = connection.execute(pin_string)
            #Executing the query and getting the selected row. 
            
            for row in pin_selected_row:
                # Iterating over the rows in the selected data.
                pin_result = dict(row._mapping)
                # Converting the row data into a dictionary for easy access.
                pin_url ='https://fbjbyb6q42.execute-api.us-east-1.amazonaws.com/Test/topics/126dc60b95b3.pin'
                #Defining the API endpoint for the pinterest data. 
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
# JSON Payload for the various values in the data. 



                response = requests.request(method="POST", url=pin_url, headers=headers, data=pin_payload)
                # Sending a POST request to the URL with the correct header and JSON payload.
                if 400 <= response.status_code < 600:
                   # Checking if the response status code is invalid.
                   print(f"Response Headers: {response.headers}")
                   # Printing the status code of the response to identify the error.
                   print(response.status_code)
                   # Printing the actual status code of the response.
                   print(f'Response Content: {response.text}')
                   # Printing the content of the response to understand the error. 
                else:
                    #Optionally, print a success message or the status code for successful requests
                    print(f"Request successful with status code: {response.status_code}")


            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            # Query to select a single row from the 'geolocation_data' table randomly.
            geo_selected_row = connection.execute(geo_string)
            # Executing the query and getting the selected row.
            
            for row in geo_selected_row:
                # Iterating over the rows in the selected data.
                geo_result = dict(row._mapping) # Converting the row data into a dictionary.
                geo_url = 'https://fbjbyb6q42.execute-api.us-east-1.amazonaws.com/Test/topics/126dc60b95b3.geo' # Defining the specific API endpoint for the geo data.
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
}) #Geo data JSON payload. 
                
                response = requests.request(method="POST", url=geo_url, headers=headers, data=geo_payload)
                # Sending a POST request to the specific URL and storing it in the 'response' variable.

                if 400 <= response.status_code < 600:
                    print(response.status_code)
                    print(f"Response Headers: {response.headers}")
                    print(f'Response Content: {response.text}')
                else:
                    print(f"Request successful with status code: {response.status_code}")




            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            # Query to select a single row from the 'user_data' table randomly.
            user_selected_row = connection.execute(user_string)
            # Executing the query and retrieving the selected row. 
            for row in user_selected_row:
                # Iterating over the rows in the selected data.
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
        } # User JSON data payload.
    ]
})
                user_url = 'https://fbjbyb6q42.execute-api.us-east-1.amazonaws.com/Test/topics/126dc60b95b3.user'
                # Defining the specific API endpoint for the USER data related to the pinterest posts.
                response = requests.request(method="POST", url=user_url, headers=headers, data=user_payload)
                # Sending a POST request to the specific URL. 
                if 400 <= response.status_code < 600:
                    print(response.status_code)
                    print(f"Response Headers: {response.headers}")
                    print(f'Response Content: {response.text}')
                else:
                    print(f"Request successful with status code: {response.status_code}")



if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
