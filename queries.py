import mysql.connector
import random
from pyfiglet import Figlet

figlet = Figlet(font="standard")
ascii_art = figlet.renderText("BOSTON CRIMES")

def execute_query(db_config, query_number):
    try:
        db = mysql.connector.connect(**db_config)
        cursor = db.cursor(buffered=True)

        if query_number == 1:
            # Query 1: Area with the most crimes by day or night
            choice = input("Do you want to know the worst area by day or by night? (day/night): ").lower()
            if choice not in ['day', 'night']:
                print("Invalid choice. Please enter 'day' or 'night'.")
                return

            time_condition = "HOUR(Incidents.Date) BETWEEN 7 AND 19" if choice == 'day' else "HOUR(Incidents.Date) NOT BETWEEN 7 AND 19"
            query = f"""
                SELECT Place.District, Place.Street, COUNT(*) as CrimeCount
                FROM Incidents
                JOIN Place ON Incidents.ID = Place.Incident_ID
                WHERE {time_condition}
                GROUP BY Place.District, Place.Street
                ORDER BY CrimeCount DESC
                LIMIT 1;
            """
            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                print(f"Most crimes in {choice} time: District: {result[0]}, Street: {result[1]}, Number of Crimes: {result[2]}, BE CAREFUL OF THESE PLACES!!")
            else:
                print(f"No crime data available for {choice} time.")

        elif query_number == 2:
            # Query 2: Crime increase or decrease year on year
            query = """
                SELECT Year, TotalCrimes,
                       (TotalCrimes - LAG(TotalCrimes) OVER (ORDER BY Year)) / LAG(TotalCrimes) OVER (ORDER BY Year) * 100 as PercentageChange
                FROM (
                    SELECT YEAR(Date) as Year, COUNT(*) as TotalCrimes
                    FROM Incidents
                    GROUP BY YEAR(Date)
                ) as YearlyCrimeData;
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            for result in results:
                year, total_crimes, percentage_change = result
                if percentage_change is not None:
                    change_description = "Increased" if percentage_change > 0 else "Decreased"
                    print(f"Year: {year}, Total Crimes: {total_crimes}, Change: {change_description} by {percentage_change:.2f}%")
                else:
                    print(f"Year: {year}, Total Crimes: {total_crimes}, Change: No previous year data")


        elif query_number == 3:
           # Query 3: Top 3 most occurring crime types
            query = """
                SELECT Crime_Types.Code, Crime_Types.Description, COUNT(Incidents.ID) as TotalOccurrences
                FROM Crime_Types
                JOIN Incidents ON Crime_Types.Code = Incidents.Offence_Code
                GROUP BY Crime_Types.Code, Crime_Types.Description
                ORDER BY TotalOccurrences DESC
                LIMIT 3;
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            print("Top 3 most occurring crime types in the last 4 years:")
            for result in results:
                code, description, count = result
                print(f"Crime type: {description}, Occurences: {count}")

        elif query_number == 4:
            # Query 4: Area and shootings for a given street
            street_name = input("Enter a street name in CAPS (leave blank for random examples): ").strip()

            if not street_name:
                cursor.execute("SELECT DISTINCT Street FROM Place ORDER BY RAND() LIMIT 3;")
                random_streets = cursor.fetchall()
                print("Random street examples:")
                for street in random_streets:
                    print(street[0])
                street_name = input("Enter one of the above street names or any other: ").strip()

            # Check if the street exists in the database
            cursor.execute("SELECT COUNT(*) FROM Place WHERE Street = %s;", (street_name,))
            if cursor.fetchone()[0] == 0:
                print("Street not in database.")
                return

            # Fetch area and shootings for the given street
            query = """
                SELECT Place.Area, COUNT(Incidents.ID) as Shootings
                FROM Place
                LEFT JOIN Incidents ON Place.Incident_ID = Incidents.ID AND Incidents.Shootings = 'Y'
                WHERE Place.Street = %s
                GROUP BY Place.Area;
            """
            
            cursor.execute(query, (street_name,))
            result = cursor.fetchone()
            if result:
                area, shootings = result
                if shootings > 0:
                    print(f"Street: {street_name}, Area: {area}, Number of Shootings: {shootings}, Careful when going here!")
                else:
                    print("Fortunately there have been no shootings in this area, you are safe to go!")
            else:
                print(f"Street found, but no area data available for: {street_name}")

        elif query_number == 5:
           # Query 5: Random fact about the crime data
            queries = [
                "SELECT COUNT(*) FROM Incidents;",  # Total number of incidents v
                "SELECT MAX(Date) FROM Incidents;",  # Most recent incident v
                "SELECT District, COUNT(*) FROM Place GROUP BY District ORDER BY COUNT(*) DESC LIMIT 1;",  # District with the most incidents v
                "SELECT Street, COUNT(*) FROM Place GROUP BY Street ORDER BY COUNT(*) DESC LIMIT 1;",  # Street with the most incidents v
                "SELECT COUNT(*) FROM Incidents WHERE Shootings = 'Y';",  # Total number of shootings v
                "SELECT YEAR(Date), COUNT(*) FROM Incidents GROUP BY YEAR(Date) ORDER BY COUNT(*) DESC LIMIT 1;",  # Year with the highest number of crimes v
                "SELECT AVG(DailyCrimes) FROM (SELECT DATE(Date) as CrimeDate, COUNT(*) as DailyCrimes FROM Incidents GROUP BY DATE(Date)) as DailyCrimeStats;",  # Average number of crimes per day v
                "SELECT MONTH(Date) as CrimeMonth, COUNT(*) FROM Incidents GROUP BY MONTH(Date) ORDER BY COUNT(*) DESC LIMIT 1;"  # Month with the most crimes v
            ]

            # Select a random query
            random_query = random.choice(queries)
            cursor.execute(random_query)
            result = cursor.fetchone()

            if result:
                if random_query == queries[0]:
                    print(f"Total number of incidents: {result[0]}")
                elif random_query == queries[1]:
                    print(f"Most recent incident date: {result[0]}")
                elif random_query == queries[2]:
                    print(f"District with the most incidents: {result[0]}, Number of Incidents: {result[1]}")
                elif random_query == queries[3]:
                    print(f"Street with the most incidents: {result[0]}, Number of Incidents: {result[1]}")
                elif random_query == queries[4]:
                    print(f"Total number of shootings: {result[0]}")
                elif random_query == queries[5]:
                    print(f"Year with the highest number of crimes: {result[0]}, Number of Crimes: {result[1]}")
                elif random_query == queries[6]:
                    print(f"Average number of crimes per day: {result[0]:.2f}")
                elif random_query == queries[7]:
                    print(f"Month with the most crimes: {result[0]}, Number of Crimes: {result[1]}")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Close the cursor and connection safely
        if cursor:
            cursor.close()
        if db and db.is_connected():
            db.close()

def main():
    # Database Configuration - Update with your actual database credentials
    db_config = {
        'host': 'localhost',
        'user': 'username',
        'passwd': 'password',
        'database': 'BostonCrimes'
    }

    while True:
        print(ascii_art)
        print("\nSelect a query to execute:")
        print("1. Which areas should I avoid at different times when visiting Boston? ")
        print("2. Is Boston becoming safer as the years pass?")
        print("3. What are the most common crimes to watch out for?")
        print("4. Planning on going to a certain street? Check if it is safe first!")
        print("5. I feel lucky")
        print("0. Exit")

        choice = input("Enter your choice (0-5): ")

        if choice == '0':
            break

        if choice in {'1', '2', '3', '4', '5'}:
            execute_query(db_config, int(choice))
        else:
            print("Invalid choice, please choose a number between 0-5.")

if __name__ == "__main__":
    main()

