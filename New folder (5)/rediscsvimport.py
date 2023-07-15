import csv
import redis


redis_host = 'localhost' 
redis_port = 6379 
redis_client = redis.Redis(host=redis_host, port=redis_port)

csv_file = "C:\\Users\\jigne\\Desktop\\data base\\student_2500.csv"

with open(csv_file, 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
       
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

        redis_client.hset(f'applicant_2500:{applicant_id}', mapping={
            'name': name,
            'gender': gender,
            'nationality': nationality,
            'email': email,
            'transcript_id': transcript_id,
            'institution_name': institution_name,
            'degree': degree,
            'year_of_graduation': year_of_graduation,
            'gpa': gpa,
            'application_status': application_status
        })
        redis_client.sadd(f'application_status:{application_status}', applicant_id)