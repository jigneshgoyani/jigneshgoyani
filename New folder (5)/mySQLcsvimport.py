import mysql.connector
import csv

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Jignesh@2002"
)

cursor = connection.cursor()

database_name = "applicant_1000"
check_database_query = f"SHOW DATABASES LIKE '{database_name}';"
cursor.execute(check_database_query)

database_exists = False
for (database,) in cursor:
    if database == database_name:
        database_exists = True
        break

if not database_exists:
    create_database_query = f"CREATE DATABASE {database_name};"
    cursor.execute(create_database_query)

use_database_query = f"USE {database_name};"
cursor.execute(use_database_query)

create_table_query = """
CREATE TABLE IF NOT EXISTS applicants_1000(
    applicant_id INT PRIMARY KEY,
    name VARCHAR(255),
    gender VARCHAR(255),
    nationality VARCHAR(255),
    email VARCHAR(255),
    transcript_id VARCHAR(255),
    institution_name VARCHAR(255),
    degree VARCHAR(255),
    year_of_graduation INT,
    gpa FLOAT,
    application_status VARCHAR(255)
);
"""
cursor.execute(create_table_query)

csv_file_path = "C:\\Users\\jigne\\Desktop\\data base\\student_1000.csv"  
with open(csv_file_path, 'r') as file:
    csv_data = csv.DictReader(file)

    insert_query = """
    INSERT INTO applicants_1000 (applicant_id, name, gender, nationality, email, transcript_id, institution_name, degree, year_of_graduation, gpa, application_status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for row in csv_data:
        cursor.execute(insert_query, (
            row['applicant_id'], row['name'], row['gender'], row['nationality'], row['email'], row['transcript_id'],
            row['institution_name'], row['degree'], row['year_of_graduation'], row['gpa'], row['application_status']
        ))

connection.commit()

cursor.close()
connection.close()