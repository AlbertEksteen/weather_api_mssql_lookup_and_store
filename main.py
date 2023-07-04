import pyodbc
import requests
import json


API_KEY = '8WEafSCWiBJA0BKigfIFaKFun4gF4PJz'
SERVER_NAME = 'Albert-ZBook'
DATABASE_NAME = 'Weather_Information'
USERNAME = 'python'
PASSWORD = 'python1234'
PORT = '1433'


def retrieve_cities_from_database():
    # Establish the database connection
    connection_string = 'DRIVER={SQL Server};SERVER=' + SERVER_NAME + ';PORT=' + PORT + ';DATABASE=' + DATABASE_NAME + ';UID=' + USERNAME + ';PWD=' + PASSWORD + ';'
    conn = pyodbc.connect(connection_string)

    # Create a cursor to execute SQL queries
    cursor = conn.cursor()

    # Execute the SQL query to retrieve data
    sql_query = 'SELECT [Id], [CountryId], [Name], [Latitude], [Longitude] FROM [dbo].[Cities]'
    cursor.execute(sql_query)

    # Fetch all rows and retrieve the column names
    rows = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]

    # Convert the result set to a list of dictionaries
    result_set = []
    for row in rows:
        result_set.append(dict(zip(column_names, row)))

    # Close the database connection
    conn.close()

    return result_set




def get_weather_location(latitude, longitude, api_key):
    url = f"http://dataservice.accuweather.com/locations/v1/cities/geoposition/search?apikey={api_key}&q={latitude},{longitude}"

    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        return data['Key'] #data[0]  # Assuming the first item in the response represents the current weather conditions
    else:
        print('API request failed with status code:', response.status_code)
        return None




def get_current_weather(locationkey, api_key):
    url = f"http://dataservice.accuweather.com/currentconditions/v1/{locationkey}?apikey={api_key}"

    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        return data #data[0]  # Assuming the first item in the response represents the current weather conditions
    else:
        print('API request failed with status code:', response.status_code)
        return None



def store_cities_json_database(json_data):
    # Establish the database connection
    connection_string = 'DRIVER={SQL Server};SERVER=' + SERVER_NAME + ';PORT=' + PORT + ';DATABASE=' + DATABASE_NAME + ';UID=' + USERNAME + ';PWD=' + PASSWORD + ';'
    conn = pyodbc.connect(connection_string)

    # Create a cursor to execute SQL queries
    cursor = conn.cursor()

    try:
        # Convert the JSON object to a string
        json_string = json.dumps(json_data)

        # Construct the SQL query
        sql_query = 'INSERT INTO [dbo].[JsonData] ([JsonDocument]) VALUES (?)'

        # Execute the SQL query with the JSON string as a parameter
        cursor.execute(sql_query, json_string)

        # Commit the changes
        conn.commit()
        print('JSON data stored successfully in the database.')
    except Exception as e:
        # Handle any exceptions that occur during the database operation
        print('Error storing JSON data in the database:', str(e))
        conn.rollback()

    # Close the database connection
    conn.close()



def main():
    api_key = API_KEY

    # Assign the function output to the dictionary
    cities = retrieve_cities_from_database()

    # Loop through the dictionary and pass multiple values to the API calling function
    for row in cities:
        latitude = row['Latitude']
        longitude = row['Longitude']

        locationkey = get_weather_location(latitude, longitude, api_key)
        #print(locationkey)

        json_data = get_current_weather(locationkey, api_key)
        #print(json_data)

        store_cities_json_database(json_data)

main()

