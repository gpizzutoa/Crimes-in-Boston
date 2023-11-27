from pymongo import MongoClient
import csv

# Establish a connection to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['crime_database']

# Collections
crime_types_col = db['crime_types']
incidents_col = db['incidents']

# Import data from CSV with batch processing
def import_data():
    file_path = 'C:/Users/User/OneDrive/Desktop/DATA_PROJECT/Crimes-in-Boston/crime.csv'
    batch_size = 1000  # Number of documents to insert at a time
    incidents_batch = []
    processed_rows = 0

    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        print("Starting data import...")
        for row in reader:
            # Process each row and create a document for the incidents collection
            incident = {
                "ID": row['INCIDENT_NUMBER'],
                "OffenceCode": row['OFFENSE_CODE'],
                "Shootings": row['SHOOTING'].strip().upper() == 'Y',  # Assuming 'Y' for shootings, change as per your data
                "DateTime": {
                    "Year": int(row['YEAR']),
                    "Month": int(row['MONTH']),
                    "DayOfWeek": row['DAY_OF_WEEK'],
                    "Hour": int(row['HOUR'])
                },
                "Place": {
                    "Street": row['STREET'],
                    "Latitude": float(row['Lat']) if row['Lat'] else None,
                    "Longitude": float(row['Long']) if row['Long'] else None,
                    "Location": row['Location']
                }
            }
            incidents_batch.append(incident)
            processed_rows += 1

            # Insert batch when it reaches the batch size
            if len(incidents_batch) == batch_size:
                incidents_col.insert_many(incidents_batch)
                incidents_batch = []
                print(f"Processed and inserted {processed_rows} rows...")

        # Insert any remaining documents in the last batch
        if incidents_batch:
            incidents_col.insert_many(incidents_batch)
            print(f"Processed and inserted {processed_rows} rows...")

    print("Data import completed.")


# Additional CRUD functions with print statements
def create_incident(incident_data):
    incidents_col.insert_one(incident_data)
    print(f"Inserted incident with ID: {incident_data['ID']}")

def read_incident(incident_id):
    incident = incidents_col.find_one({"ID": incident_id})
    print(f"Read incident: {incident}")
    return incident

def update_incident(incident_id, update_data):
    incidents_col.update_one({"ID": incident_id}, {"$set": update_data})
    print(f"Updated incident with ID: {incident_id}")

def delete_incident(incident_id):
    incidents_col.delete_one({"ID": incident_id})
    print(f"Deleted incident with ID: {incident_id}")

# Import data
import_data()

# Example usage of CRUD functions
# ... (you can add examples here as you did before)

# Query 1: Count of Incidents by Year
def query_incidents_by_year():
    result = incidents_col.aggregate([
        {"$group": {"_id": "$DateTime.Year", "total_incidents": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ])
    for item in result:
        print(f"Year: {item['_id']}, Total Incidents: {item['total_incidents']}")

# Query 2: Average Number of Incidents by Day of Week
def query_avg_incidents_by_day():
    result = incidents_col.aggregate([
        {"$group": {"_id": "$DateTime.DayOfWeek", "average_incidents": {"$avg": 1}}},
        {"$sort": {"average_incidents": -1}}
    ])
    for item in result:
        print(f"Day of Week: {item['_id']}, Average Incidents: {item['average_incidents']}")

# Query 3: Top 5 Most Common Crime Types
def query_top_crime_types():
    result = incidents_col.aggregate([
        {"$group": {"_id": "$OffenceCode", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ])
    for item in result:
        print(f"Offence Code: {item['_id']}, Count: {item['count']}")

# Query 4: Incidents Involving Shootings by District
def query_shootings_by_district():
    result = incidents_col.aggregate([
        {"$match": {"Shootings": True}},
        {"$group": {"_id": "$Place.District", "shootings_count": {"$sum": 1}}},
        {"$sort": {"shootings_count": -1}}
    ])
    for item in result:
        print(f"District: {item['_id']}, Shootings Count: {item['shootings_count']}")

# Execute queries
print("\nExecuting queries...")
query_incidents_by_year()
query_avg_incidents_by_day()
query_top_crime_types()
query_shootings_by_district()



