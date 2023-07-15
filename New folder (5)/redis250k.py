import redis
import time


redis_host = 'localhost' 
redis_port = 6379  
redis_client = redis.Redis(host=redis_host, port=redis_port)



def iterate_applicants():
    keys = redis_client.keys("applicant_2500:*")

    for key in keys:
        applicant_data = redis_client.hgetall(key)
        #print(applicant_data)

for i in range(30):
    start_time = time.time()
    student_count = iterate_applicants()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}:Execution Time = {execution_time} seconds")

def count_medical_students():
    medical_student_count = 0
    keys = redis_client.keys("applicant_2500:*")
    for key in keys:
        applicant_data = redis_client.hgetall(key)
        if (applicant_data[b'degree'].decode().lower() == 'medical' and
                applicant_data[b'gender'].decode().lower() == 'female' and
                applicant_data[b'nationality'].decode().lower() == 'usa' and
                applicant_data[b'institution_name'].decode().lower() == 'nyc'):
            medical_student_count += 1
    return medical_student_count

for i in range(30):
    start_time = time.time()
    student_count = count_medical_students()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}: Medical Student Count = {student_count}, Execution Time = {execution_time} seconds")


def get_transcript_ids():
    keys = redis_client.keys('applicant_2500*')
    transcript_ids = []
    for key in keys:
        applicant_data = redis_client.hgetall(key)
        gender = applicant_data.get(b'gender').decode('utf-8')
        year_of_graduation = int(applicant_data.get(b'year_of_graduation'))
        application_status = applicant_data.get(b'application_status').decode('utf-8')
        gpa = float(applicant_data.get(b'gpa'))

        if (gender == 'male' and
            year_of_graduation == 2010 and
            application_status == 'submitted' and
            gpa > 8):
            transcript_id = applicant_data.get(b'transcript_id').decode('utf-8')
            transcript_ids.append(transcript_id)

    return transcript_ids

for i in range(30):
    start_time = time.time()
    student_count = get_transcript_ids()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}:Execution Time = {execution_time} seconds")

def get_applicant_ids():
    applicant_ids = []
    keys = redis_client.keys('applicant_2500*')
    for key in keys:
        applicant_data = redis_client.hgetall(key)

        if (applicant_data.get(b'applicationStatus') == b'rejected' and
                applicant_data.get(b'institution_name') == b'messina' and
                applicant_data.get(b'degree') == b'data analyst' and
                applicant_data.get(b'nationality') == b'italy'):
            applicant_ids.append(applicant_data.get(b'applicant_id').decode())

    return applicant_ids

for i in range(30):
    start_time = time.time()
    student_count = get_applicant_ids()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}:Execution Time = {execution_time} seconds")