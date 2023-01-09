import os
from dotenv import load_dotenv , find_dotenv

#connection string param
# 1.user,password,hostname:port dbname
# set
# os.environ['username'] = "razee"
#
#
# # get
# user=os.getenv('username')
# print(user)


load_dotenv(find_dotenv())
user=os.getenv('USER_NAME')
password=os.getenv('PASS_WORD')
localhost=os.getenv('LOCALHOST')
port=os.getenv('PORT')
dbname=os.getenv('DBNAME')
print(user)
print(password)
print(localhost)
print(port)
print(dbname)