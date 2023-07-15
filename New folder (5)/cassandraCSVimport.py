from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1']) 
session = cluster.connect('applicant') 

csv_file_path = r"C:\Users\jigne\Desktop\data base\student_5000.csv"  

with open(csv_file_path, 'r') as file:
    header = file.readline().strip()
    columns = header.split(',')

    for line in file:
        data = line.strip().split(',')

        data[0] = int(data[0]) 
        data[8] = int(data[8])  
        data[9] = float(data[9])  

        insert_query = f"INSERT INTO applicant_5000 ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        session.execute(insert_query, data)

session.shutdown()
cluster.shutdown()