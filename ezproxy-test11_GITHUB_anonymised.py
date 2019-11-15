# Get data from Ezproxy logs
# Augment it with a user's school and department affiliation from Alma
# Then save it to a MySQL database for subsequent reporting/analysis/visualisation

# Tim Graves
# University of Sussex Library
# 15/11/2019

# This version of the script has any University of Sussex specific details removed: logins, keys, etc.
######################################################################################################

# We will need these imported for various activities
import mysql.connector
from datetime import datetime
import requests
import json


def getAlmaDetails(user_name):

    r = requests.get("<<URL OF ALMA API, ALONG WITH OUR API KEY TO ALLOW ACCESS>>" % (user_name,))

    # parse the results from Alma:
    alma_data = json.loads(r.text)

    # Course - in the case of students only - might be helpful
    courseArray = alma_data["job_description"]
    course = courseArray.split(" ")
    courseReturned = course[0]

    # user_group
    user_group = alma_data["user_group"]["desc"]

    # School
    school  = alma_data["user_statistic"][0]["statistic_category"]["desc"]
	
    return (courseReturned, school, user_group)


# Database connection for the database to which you will write
outputDB = mysql.connector.connect(
  host="<<MYSQL_SERVER>>",
  user="<<MYSQL-USER>>",
  passwd="<<MYSQL-PASSWORD>>",
  database="<<MYSQL-DATABASE>>"
)

myOutputcursor = outputDB.cursor()
# end of the database connection


# Function to look up geographic location based on IP

def getIP(ip_address):
    
    r = requests.get("https://ipvigilante.com/%s" % (ip_address,))
	
    #print(r)
	
    parsed_json = json.loads(r.content)
    	
    country = parsed_json["data"]["country_name"]

    return country


# Function to write to database

def writeOut(School,timestamp,resource,country,usergroup,course):

    sql2 = "INSERT INTO ezproxy_logs (school, timestamp,resource,country,usergroup,course) VALUES (%s, %s, %s, %s, %s, %s)"
    val2 = (School, timestamp, resource, country, usergroup, course)
    myOutputcursor.execute(sql2, val2)

    outputDB.commit()

    
# Read in the contents of a file. Iterate each line and split

f = open("ezproxy.out", "r")

# Show the start time
print(datetime.now())

for t in f:
    a,b,c,d = t.split(" ")
	
	# Only print if a username is included, and is not a hyphen
    #if b:
    if b and b != '-':
        person = b
        resource = d.rstrip()
		# reformat the date so that MySQL can recognise it
        timestamp = c.replace('[', '')
        timestamp = datetime.strptime(timestamp, "%d/%b/%Y:%H:%M:%S")
	
        
    # Write to the database
        try:
            userCourse = getAlmaDetails(person)[0]			 
            userSchool = getAlmaDetails(person)[1]			 
            userGroup = getAlmaDetails(person)[2]
            #writeOut(person,userSchool,timestamp,resource,getIP(a),userGroup,userCourse)
            writeOut(userSchool,timestamp,resource,getIP(a),userGroup,userCourse)
            #print(userSchool,timestamp,resource,getIP(a),userGroup,userCourse)
            
        except:
		   print("Error - skipping")


# Show the end time
print(datetime.now())