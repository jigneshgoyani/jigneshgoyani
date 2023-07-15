from cassandra.cluster import Cluster
import time

cluster = Cluster(['127.0.0.1'])  
session = cluster.connect('applicant')  

def iterate_applicants():
    query = "SELECT * FROM applicant_7500"

    result_set = session.execute(query)
    for row in result_set:
        applicant_data = row._asdict()

for i in range(30):
    start_time = time.time()
    student_count =iterate_applicants()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}:Execution Time = {execution_time} seconds")

def count_medical_students():
    query = """SELECT COUNT(*) FROM applicant_7500
    WHERE degree = 'Medical' AND gender = 'Female' AND nationality = 'USA' AND institution_name = 'nyc'ALLOW FILTERING"""

    result = session.execute(query)

    medical_student_count = result[0].count
    return medical_student_count

student_count = count_medical_students()


for i in range(30):
    start_time = time.time()
    student_count = count_medical_students()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}: Medical Student Count = {student_count}, Execution Time = {execution_time} seconds")

def get_transcript_ids():
    query = """SELECT transcript_id FROM applicant_7500 WHERE gender = 'male' AND year_of_graduation = 2010
    AND application_status = 'submitted' AND gpa > 8 ALLOW FILTERING"""

    result_set = session.execute(query)
    transcript_ids = []
    for row in result_set:
        transcript_id = row.transcript_id
        transcript_ids.append(transcript_id)

    return transcript_ids


for i in range(30):
    start_time = time.time()
    transcript_ids = get_transcript_ids()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}: Execution Time = {execution_time} seconds")


def get_applicant_ids():
    query = """ SELECT applicant_id FROM applicant_7500
    WHERE application_status = 'rejected' AND institution_name = 'messina'
    AND degree = 'data analyst' AND nationality = 'italy' ALLOW FILTERING"""

    result_set = session.execute(query)

    applicant_ids = []
    for row in result_set:
        applicant_id = row.applicant_id
        applicant_ids.append(applicant_id)

    return applicant_ids


for i in range(30):
    start_time = time.time()
    student_count =get_applicant_ids()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}:Execution Time = {execution_time} seconds")