from neo4j import GraphDatabase
import time


uri = "bolt://localhost:7687" 
username = "neo4j"
password = "112345678"  



def iterate_applicants():
    driver = GraphDatabase.driver(uri, auth=(username, password))
    query = """MATCH (a:applicant_1000) RETURN a """

    with driver.session() as session:
        result = session.run(query)

        for record in result:
            applicant_data = record['a']
    driver.close()


for i in range(30):
    start_time = time.time()
    iterate_applicants()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}: Execution Time = {execution_time} seconds")


def count_medical_students():
    medical_student_count = 0
    driver = GraphDatabase.driver(uri, auth=(username, password))

    query = """MATCH (a:applicant_1000) WHERE a.degree = 'Medical' AND a.gender = 'Female' AND a.nationality = 'USA'
    AND a.institution_name = 'nyc' RETURN COUNT(*) AS count"""

    with driver.session() as session:
        result = session.run(query)
        record = result.single()
        if record:
            medical_student_count = record["count"]
    driver.close()

    return medical_student_count

for i in range(30):
    start_time = time.time()
    student_count = count_medical_students()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}: Medical Student Count = {student_count}, Execution Time = {execution_time} seconds")

def get_transcript_ids():
    driver = GraphDatabase.driver(uri, auth=(username, password))

    query = """MATCH (a:applicant_1000) WHERE a.gender = 'male'AND a.year_of_graduation = 2010
      AND a.application_status = 'submitted' AND a.gpa > 8 RETURN a.transcript_id AS transcript_id"""

    transcript_ids = []
    with driver.session() as session:
        result = session.run(query)
        for record in result:
            transcript_ids.append(record["transcript_id"])
    driver.close()

    return transcript_ids


for i in range(30):
    start_time = time.time()
    student_count =get_transcript_ids()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}:Execution Time = {execution_time} seconds")

def get_applicant_ids():
    driver = GraphDatabase.driver(uri, auth=(username, password))

    query = """MATCH (a:applicant_1000) WHERE a.application_status = 'rejected'AND a.institution_name = 'messina'
    AND a.degree = 'data analyst' AND a.nationality = 'italy' RETURN a.applicant_id AS applicant_id"""

    with driver.session() as session:
        result = session.run(query)
        applicant_ids = [record['applicant_id'] for record in result]

    driver.close()

    return applicant_ids

for i in range(30):
    start_time = time.time()
    _ = get_applicant_ids()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}: Execution Time = {execution_time} seconds")