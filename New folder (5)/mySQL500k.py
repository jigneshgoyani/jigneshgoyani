import mysql.connector
import time

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Jignesh@2002",
    database="applicant_2500"  
)




def count_medical_students():
    medical_student_count = 0

    cursor = connection.cursor()
    query = """
    SELECT COUNT(*) FROM applicants_5000
    WHERE degree = 'Medical' AND gender = 'Female' AND nationality = 'USA' AND institution_name = 'NYC'
    """

    cursor.execute(query)
    result = cursor.fetchone()
    if result:
        medical_student_count = result[0]

    cursor.close()
    return medical_student_count

for i in range(30):
    start_time = time.time()
    student_count = count_medical_students()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}: Medical Student Count = {student_count}, Execution Time = {execution_time} seconds")



def get_transcript_ids():
    cursor = connection.cursor()

    query = """
    SELECT transcript_id
    FROM applicants_5000
    WHERE gender = 'male'
        AND year_of_graduation = 2010
        AND application_status = 'submitted'
        AND gpa > 8
    """
    cursor.execute(query)
    transcript_ids = [row[0] for row in cursor.fetchall()]

    return transcript_ids

for i in range(30):
    start_time = time.time()
    student_count =get_transcript_ids()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}:Execution Time = {execution_time} seconds")




def get_applicant_ids():
    cursor = connection.cursor()

    query = """
    SELECT applicant_id
    FROM applicants_5000
    WHERE application_status = 'rejected'
      AND institution_name = 'messina'
      AND degree = 'data analyst'
      AND nationality = 'Italy'
    """
    cursor.execute(query)
    applicant_ids = [row[0] for row in cursor]

    cursor.close()
    return applicant_ids

for i in range(30):
    start_time = time.time()
    student_count =get_applicant_ids()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}:Execution Time = {execution_time} seconds")



def iterate_applicants():
    cursor = connection.cursor()

    query = "SELECT * FROM applicants_5000"
    cursor.execute(query)

    for row in cursor:
        applicant_data = dict(zip(cursor.column_names, row))
    cursor.close()


for i in range(30):
    start_time = time.time()
    iterate_applicants()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}: Execution Time = {execution_time} seconds")

    
cursor = connection.cursor()