import pandas as pd
from neo4j import GraphDatabase


uri = "bolt://localhost:7687"
username = "neo4j"
password = "112345678"  

driver = GraphDatabase.driver(uri, auth=(username, password))

data = pd.read_csv('C:\\Users\\jigne\\Desktop\\data base\\student_1000.csv')

with driver.session() as session:
    
    for _, row in data.iterrows():
        applicant_id = row['applicant_id']
        name = row['name']
        gender = row['gender']
        nationality = row['nationality']
        email = row['email']
        transcript_id = row['transcript_id']
        institution_name = row['institution_name']
        degree = row['degree']
        year_of_graduation = row['year_of_graduation']
        gpa = row['gpa']
        application_status = row['application_status']

        cypher_query = '''
        CREATE (:applicant_1000 {applicant_id: $applicant_id, name: $name, gender: $gender, nationality: $nationality,
                          email: $email, transcript_id: $transcript_id, institution_name: $institution_name,
                          degree: $degree, year_of_graduation: $year_of_graduation, gpa: $gpa,
                          application_status: $application_status})
        '''
        session.run(cypher_query, applicant_id=applicant_id, name=name, gender=gender, nationality=nationality,
                    email=email, transcript_id=transcript_id, institution_name=institution_name,
                    degree=degree, year_of_graduation=year_of_graduation, gpa=gpa, application_status=application_status)
driver.close()
