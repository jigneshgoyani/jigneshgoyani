import pymongo
import time

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["applicant"]  
collection = db["applicant_1000"] 

def count_medical_students():
    medical_student_count = 0

    query = {"degree": "Medical","gender": "Female","nationality": "USA","institution_name": "nyc"}

    medical_students = collection.count_documents(query)

    medical_student_count = medical_students

    return medical_student_count
#student_count = count_medical_students()
#print("Medical Student Count:", student_count)

for i in range(30):
    start_time = time.time()
    student_count = count_medical_students()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}: Medical Student Count = {student_count}, Execution Time = {execution_time} seconds")

def iterate_applicants():
    applicant_ = collection.find()
    applicants = []

    for applicant in applicant_:
       
        applicants.append(applicant)

    return applicants

#all_applicants = iterate_applicants()
#print(all_applicants)

for i in range(30):
    start_time = time.time()
    student_count =iterate_applicants()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}:Execution Time = {execution_time} seconds")





def get_applicant_ids():
    query = {"application_status": "rejected","institution_name": "messina","degree": "data analyst","nationality": "italy"}

    applicant_ids = []
    for doc in collection.find(query):
        applicant_ids.append(doc["applicant_id"])

    return applicant_ids

for i in range(30):
    start_time = time.time()
    transcript_ids =get_applicant_ids()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}: Execution Time = {execution_time} seconds")




def get_transcript_ids():
    query = {"gender": "Male","year_of_graduation": 2010,"application_status": "submitted","gpa": {"$gt": 8}}

    transcript_ids = []
    for doc in collection.find(query):
        transcript_ids.append(doc["transcript_id"])
        #transcript_id = doc["transcript_id"]
        #print(transcript_id)

    return transcript_ids

for i in range(30):
    start_time = time.time()
    transcript_ids =get_transcript_ids()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Run {i + 1}: Execution Time = {execution_time} seconds")