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
        # Method to create and return a database engine connection.
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector() # Creating an instance of the connection.


def run_infinite_post_data_loop():
    # Function to run an infinite loop to post data periodically. 
    while True: #Starting the infinite loop.
        sleep(random.randrange(0, 2)) # Pausing the execution for a random time between 0 and 2 seconds.
        random_row = random.randint(0, 11000) # Selecting a random row number.
        engine = new_connector.create_db_connector() # Creating a new connection to the database.  
        headers= {'Content-Type': 'application/vnd.kafka.json.v2+json'}  # Header for the HTTP requests

        def serialize_datetime(o):
            # Function to serialise the datetime objects into ISO format.
            if isinstance(o, datetime.datetime) : 
                return o.isoformat() # Returning the ISO format of the datetime objct.
            raise TypeError("Type not serializable")# Raise error if the object is invalid.


        with engine.connect() as connection:
            # Establishing a connection to the database.

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            # Query to select a single row from the 'pinterest_data' table randomly. 
            pin_selected_row = connection.execute(pin_string)
            #Executing the query and getting the selected row.
            
            for row in pin_selected_row:
                # Iterating over the rows in the selected data.
                pin_result = dict(row._mapping)
                # Converting the row data into a dictionary for easy access.
                pin_streaming_payload = json.dumps({
                    # Defining the name of the stream for the payload.
                    "StreamName": "streaming-126dc60b95b3-pin",
                    'Data': {
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
        }, 'PartitionKey': (pin_result['index']) # Including 'PartitionKey' in the payload using the 'index' from the pin_Result.
})
      
                streaming_response = requests.request("PUT", url= "https://fbjbyb6q42.execute-api.us-east-1.amazonaws.com/Test/streams/streaming-126dc60b95b3-pin/record", headers={'Content-Type': 'application/json'}, data= pin_streaming_payload)
                # Submitting a PUT request to the specific URL for data streaming for the pinterest listing info, specifiying the content type and the payload.
                print(streaming_response.status_code)
                # Printing the status code of the streaming reponse to monitor the code. 
                print(f"Response Headers: {streaming_response.headers}")
                # Printing the header of the status code.
                print(f'Response Content: {streaming_response.text}')
                # Printing the content of the status code to understand the result. 

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            # Query to select a single row from the 'geolocation_data' table randomly.
            geo_selected_row = connection.execute(geo_string)
            # Executing the query and retrieving the selected row for geolocation data.
            for row in geo_selected_row:
                # Iterating over the rows in the selected geolocation data.
                geo_result = dict(row._mapping)
                # Converting the row data into a dictionary.

                geo_streaming_payload = json.dumps({
                    # Definining the name of the stream for geolocation payload.
                    "StreamName":"streaming-126dc60b95b3-geo",
    'Data':
            {
                'ind': geo_result['ind'],
                'timestamp': geo_result['timestamp'].isoformat(),
                'latitude': geo_result['latitude'],
                'longitude': geo_result['longitude'],
                'country': geo_result['country']
            }, "PartitionKey": (geo_result['ind']) # Including 'PartitionKey' in the payload using the 'ind' from geo_result.
})
                streaming_response = requests.request("PUT", url= "https://fbjbyb6q42.execute-api.us-east-1.amazonaws.com/Test/streams/streaming-126dc60b95b3-geo/record", headers={'Content-Type': 'application/json'}, data= geo_streaming_payload)
                # Submitting a PUT request to the specific URL for geo data streaming for the pinterest listing info, specifiying the content type and the payload.
                print(streaming_response.status_code)
                print(f" Geo Response Headers: {streaming_response.headers}")
                print(f' Geo Response Content: {streaming_response.text}')



            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            # Query to select a single row from the 'user_data' table randomly.
            user_selected_row = connection.execute(user_string)
            # Executing the query and retrieving the selected row for user data.
            
            for row in user_selected_row:
                # Iterating over the rows in the selected user data.
                user_result = dict(row._mapping)
                user_streaming_payload = json.dumps({
                # Definining the name of the stream for user payload.
                "StreamName": "streaming-126dc60b95b3-user",
    'Data': {
                'ind': user_result['ind'],
                'first_name': user_result['first_name'],
                'last_name': user_result['last_name'],
                'age': user_result['age'],
                'date_joined': user_result['date_joined'].isoformat()
            }, "PartitionKey": (user_result['ind']) # Including 'PartitionKey' in the payload using the 'ind' from user_result.
})
                streaming_response = requests.request("PUT", url= "https://fbjbyb6q42.execute-api.us-east-1.amazonaws.com/Test/streams/streaming-126dc60b95b3-user/record", headers={'Content-Type': 'application/json'}, data= user_streaming_payload)
                # Submitting a PUT request to the specific URL for data streaming for the pinterest listing info, specifiying the content type and the payload.
                print(streaming_response.status_code)
                print(f"User Response Headers: {streaming_response.headers}")
                print(f'User Response Content: {streaming_response.text}')


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
