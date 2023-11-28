import mysql.connector as mysql
from mysql.connector import Error

# MySQL DB credentials
user = "usernmae"
password = "password"

def create_database(cursor):
    try:
        cursor.execute("CREATE DATABASE BostonCrimes")
        print("Database created successfully")
    except Error as e:
        print(f"Error creating database: {e}")

def create_tables(cursor):
    # Crime Types table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Crime_Types (
        Code VARCHAR(255),
        Code_Group VARCHAR(255),
        Description VARCHAR(255) NOT NULL,
        UCR VARCHAR(255),
        PRIMARY KEY (Code, Code_Group)
    )
    """)

    # Incidents table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Incidents (
        ID VARCHAR(20) PRIMARY KEY,
        Offence_Code VARCHAR(255) NOT NULL,
        Shootings BOOLEAN,
        Date DATETIME NOT NULL
    )
    """)

    # Place table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Place (
        Street VARCHAR(255),
        District VARCHAR(255),
        Area VARCHAR(255),
        Location VARCHAR(255) NOT NULL,
        Latitude DECIMAL(10, 8),
        Longitude DECIMAL(11, 8),
        Incident_ID VARCHAR(20),
        FOREIGN KEY (Incident_ID) REFERENCES Incidents(ID)
    )
    """)

    print("Tables created successfully")

def main():
    try:
        # Connect to MySQL Server
        db = mysql.connect(host="localhost", user=user, passwd=password)
        cursor = db.cursor()

        # Create database
        create_database(cursor)

        # Connect to the newly created database
        db.database = 'BostonCrimes'

        # Create tables
        create_tables(cursor)

    except Error as e:
        print(f"Error: {e}")
    finally:
        if db.is_connected():
            cursor.close()
            db.close()
            print("MySQL connection is closed")

if __name__ == "__main__":
    main()

import mysql.connector as mysql
import csv

# Connect to the MySQL Database
db = mysql.connect(
    host="localhost",
    user="username",  # Replace with your username
    passwd="password",  # Replace with your password
    database="bostoncrimes"  # Database name
)
cursor = db.cursor()

# Function to insert data into Crime_Types and Incidents
def insert_data(row):
    try:
        # Prepare data for Crime_Types and Incidents
        crime_type_data = (row['OFFENSE_CODE'], row['OFFENSE_CODE_GROUP'], row['OFFENSE_DESCRIPTION'], row['UCR_PART'])
        incident_data = (row['INCIDENT_NUMBER'], row['OFFENSE_CODE'], 'Y' if row['SHOOTING'] == 'Y' else None, row['OCCURRED_ON_DATE'])
        place_data = (row['STREET'], row['DISTRICT'], row['REPORTING_AREA'], row['Location'], row['INCIDENT_NUMBER'])

        # Insert into Crime_Types
        cursor.execute("INSERT IGNORE INTO Crime_Types(Code, Code_Group, Description, UCR) VALUES (%s, %s, %s, %s)", crime_type_data)
    
        # Insert into Incidents
        cursor.execute("INSERT IGNORE INTO Incidents(ID, Offence_Code, Shootings, Date) VALUES (%s, %s, %s, %s)", incident_data)

        #Insert  into Place
        cursor.execute("INSERT INTO Place(Street, District, Area, Location, Incident_ID) VALUES (%s, %s, %s, %s, %s)", place_data)
        
        if csv_reader.line_num % 100 == 0:
            db.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        print(f"Error row: {row}")



# Read CSV and insert data
with open('C:/Users/Gianfranco Pizzuto/OneDrive/Escritorio/DATABASES-PROJECT/Crimes-in-Boston/crime.csv', 'r', encoding='cp1252') as file:
    csv_reader = csv.DictReader(file)

    for row in csv_reader:
        insert_data(row)

    # Final commit for any remaining rows
    db.commit()

# Close the connection
cursor.close()
db.close()
