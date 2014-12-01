from time import sleep
import sys
import pprint, pickle
import operator
import mysql.connector
import os
import re
from datetime import date, timedelta
from datetime import datetime

import ConfigParser

config = ConfigParser.ConfigParser()

config.read('../config')

DATABASE = config.get('mysql','DATABASE')
HOST = config.get('mysql','HOST')
USERNAME = config.get('mysql','USERNAME')
PASSWORD = config.get('mysql','PASSWORD')


SERIALIZATION_PATH = config.get('DEFAULT','serializationpath')

videodir = config.get('DEFAULT','externalfilespath')+'/videos'
quizdir = config.get('DEFAULT','externalfilespath')+'/quizzes'



#screen Output options
screenOutput = True

printLogins = False
printLogouts = False
printQuizzesStarted = False
printQuizzesEnded = False
printDownloads = False
printForumRead = False
printForumWritten = False

printDeserializationProgress = True

serializeData = True



# Run a normal search--search everything, return 1st 10 events
ignored_rooms=["..","mainOwp","server"]
#splunk_jobs = service.jobs

#lookup token->username
logins_tokens = dict()

#dicts for displaying purposes
logins_number = dict()
quizzes_number = dict()
quizzes_per_user= dict()
downloads_number = dict()
downloads_per_user = dict()
forum_written = dict()
forum_read = dict()
forum_written_per_user = dict()
forum_read_per_user = dict()
courses_array = []
quizzes_array = []

#mysql connection
cnx = mysql.connector.connect(user=USERNAME,password=PASSWORD, database=DATABASE, host=HOST)
curA = cnx.cursor(buffered=True)








#function to filter a specific string in between 2 given delimiters
def filterString(raw,first_delimiter,second_delimiter):
        result = raw.split(first_delimiter)
        result = result[1]
        result = result.split(second_delimiter)
        result = result[0]
        return result

def write2file(data, destination):
	first_line = ''
	second_line = ''
	#print "NUMBER OF LOGINS PER USER: "
	for item in data:
		first_line = first_line+item[0]+" "
		second_line = second_line+str(item[1])+" " 
		#print item
	#print first_line
	#print second_line
	
	f = open(destination,'w')
	f.write(first_line.strip()+'\n') # python will convert \n to os.linesep
	f.write(second_line.strip())
	f.close()

def parseLogin(result,time,previous_time, previous_login_username):
	
	if previous_time != None:
		last_minutes = previous_time.split("T")[1].split(":")[1]
	else:
		last_minutes = "None"

	recent_minutes = time.split("T")[1].split(":")[1]
	
	if last_minutes != recent_minutes or (last_minutes == recent_minutes and (result['login'] != previous_login_username)): # if not in the same minute it's OK, if in the same minute, usernames must be different
		last_event = "LOGIN"
		if screenOutput and printLogins:
			print "LOGIN at ",time ,"   by ", result['login']
		if len(result['login']) < 50 and len(result['token']) < 45: #too long usernames
			logins_tokens[result['token']] = result['login']
			insert_new_user = ("REPLACE INTO users  (uname, token, last_time) VALUES (%s,%s,%s)")
			curA.execute(insert_new_user,( result['login'],result['token'],time))
			insert_login = ("INSERT INTO logins (uname, time) VALUES (%s,%s)")
			curA.execute(insert_login,(result['login'], time))
			cnx.commit()
			
			if result['login'] in  logins_number:
				logins_number[result['login']] += 1
			else:
				logins_number[result['login']] = 1
		else:
			print "Username "+result['login']+" too long, max 50 chars"
				
def parseLogout(result,time):
	if screenOutput and printLogouts:
		if result['token'] in logins_tokens.keys():
			print "LOGOUT by         ", logins_tokens[result['token']]," at ",time
		else:
			print "LOGOUT by         ", result['token'], " at ", time
	
def parseQuizStarted(result,time,previous_time,previous_time_quiz_started_username):
        
	if previous_time != None:
		last_minutes = previous_time.split("T")[1].split(":")[1]
	else:
		last_minutes = "None"

	recent_minutes = time.split("T")[1].split(":")[1]

	username=filterString(result['uri'],'mainOwp/','_')
	quizname=filterString(result['uri'], username+'_', '.scorm')
	coursename=quizname.split("_")[0]	

	if last_minutes != recent_minutes or (last_minutes == recent_minutes and (username != previous_time_quiz_started_username)): # if not in the same minute it's OK, if in the same minute, usernames must be different
		if screenOutput and printQuizzesStarted:
			print "QUIZ           ", quizname,  "started by ", username," at ", time
		if quizname in  quizzes_number:
			quizzes_number[quizname] += 1
		else:
			quizzes_number[quizname] = 1
	
		if username in quizzes_per_user:
			quizzes_per_user[username] +=1
		else:
			quizzes_per_user[username] = 1
	
		if len(quizname) < 80 and len(username) < 80:
			insert_quiz_started = ("INSERT INTO quizzes_started (qname, uname, time) VALUES (%s,%s,%s)")
			curA.execute(insert_quiz_started,(quizname,username,time))
			cnx.commit()
		
def parseQuizEnded(result,time,previous_time):
               
	username=filterString(result['uri'],'mainOwp/','_')
	quizname=filterString(result['uri'], username+'_', '.scorm')
	if screenOutput and printQuizzesEnded:
		print "QUIZ         ", quizname, "ended by ", username," at ", time

def parseDownload(result,time,previous_time, previous_time_download_username):
	if previous_time != None:
		last_minutes = previous_time.split("T")[1].split(":")[1]
	else:
		last_minutes = "None"

	recent_minutes = time.split("T")[1].split(":")[1]

	
	if result['token'] in logins_tokens.keys():
		if last_minutes != recent_minutes or (last_minutes == recent_minutes and (logins_tokens[result['token']] != previous_time_download_username)): # if not in the same minute it's OK, if in the same minute, usernames must be different
	
			#if "token" in result.keys() and result['token'] in logins_tokens.keys():
	
			filename = 'unknown'
			for course in courses_array:
				course_adapted = course + "/"
				if course_adapted in result['uri']:
					filename=filterString(result['uri'],course_adapted,'?token')
					if len(filename) < 80 and len(course) < 80:
						curA.execute("INSERT IGNORE INTO files (fname,cname) VALUES (%s,%s)",(filename,course))
						cnx.commit()
					if len(filename) < 80 and len(logins_tokens[result['token']]) < 80:
						curA.execute("INSERT INTO files_downloaded (fname,uname,time) VALUES (%s,%s,%s)",(filename,logins_tokens[result['token']],time))
						cnx.commit()	
			#print filename
		
			if screenOutput and printDownloads:
				print "DOWNLOAD of file ",filename, "by ",logins_tokens[result['token']], "at ",time
			if filename in downloads_number:
				downloads_number[filename] +=1
			else: 
				downloads_number[filename] = 1
		
			if logins_tokens[result['token']] in downloads_per_user:
				downloads_per_user[logins_tokens[result['token']]] += 1
			else:
				downloads_per_user[logins_tokens[result['token']]] = 1

def parseForumWritten(result,time,previous_time, previous_time_forum_written_username):

	if previous_time != None:
		last_minutes = previous_time.split("T")[1].split(":")[1]
	else:
		last_minutes = "None"

	recent_minutes = time.split("T")[1].split(":")[1]

	if result['token'] in logins_tokens.keys():
		if last_minutes != recent_minutes or (last_minutes == recent_minutes and (logins_tokens[result['token']] != previous_time_forum_written_username)): # if not in the same minute it's OK, if in the same minute, usernames must be different
			#if "token" in result.keys() and result['token'] in logins_tokens.keys():	
			forumname=filterString(result['uri'],"room=","&")
			if screenOutput and printForumWritten:
				print "FORUM entry written in ",filterString(result['uri'],"room=","&")," by ", logins_tokens[result['token']], "   at ", time
			if forumname in forum_written:
				forum_written[forumname] +=1
			else:
				forum_written[forumname] = 1
		
			if logins_tokens[result['token']] in forum_written_per_user:
				forum_written_per_user[logins_tokens[result['token']]] += 1
			else:
				forum_written_per_user[logins_tokens[result['token']]] = 1
			#if forumname in courses_array:
			if len(forumname) < 80 and len(logins_tokens[result['token']]) < 80: 
				curA.execute("INSERT INTO forums_written (fname,uname,time) VALUES (%s,%s,%s)",(forumname,logins_tokens[result['token']],time))
				cnx.commit()

def parseForumRead(result,time,previous_time, previous_time_forum_read_username):
	if previous_time != None:
		last_minutes = previous_time.split("T")[1].split(":")[1]
	else:
		last_minutes = "None"

	recent_minutes = time.split("T")[1].split(":")[1]

	
	
	if result['token'] in logins_tokens.keys():
		if last_minutes != recent_minutes or (last_minutes == recent_minutes and (logins_tokens[result['token']] != previous_time_forum_read_username)): # if not in the same minute it's OK, if in the same minute, usernames must be different
	
			room=filterString(result['uri'],"file=","/")
			
			if result['token'] in logins_tokens.keys() and room not in ignored_rooms:
				#saveCourseName(room) #TODO: welche raeume gibts da alles??
				if room in forum_read:
					forum_read[room] +=1
				else:
					forum_read[room] = 1
		
				if logins_tokens[result['token']] in forum_read_per_user:
					forum_read_per_user[logins_tokens[result['token']]] += 1
				else:
					forum_read_per_user[logins_tokens[result['token']]] = 1
				if screenOutput and printForumRead:
					print "FORUM entry in course ", room," read by ", logins_tokens[result['token']],"   at ",time
		
				if room in courses_array:
					if len(room) < 80:	
						curA.execute("INSERT IGNORE INTO forums (fname,cname) VALUES (%s,%s)",(room,room))
						cnx.commit()
					if len(room) < 80 and len(logins_tokens[result['token']]) < 80:
						curA.execute("INSERT INTO forums_read (fname,uname,time) VALUES (%s,%s,%s)",(room,logins_tokens[result['token']],time))
						cnx.commit()

def parseCourseNames():
	#Scan existing quizzes and existing courses
	for file in os.listdir(quizdir):
		if file.endswith(".txt"):
			# file
			
			coursename = '_'.join(file.split("_")[0:len(file.split("_"))-1])
			#print coursename

			saveCourseName(coursename)		
			destination = quizdir+"/"+str(file)
			f = open(destination, 'r')
			for line in f:
				quizzes = line.split(";")
				for quiz in quizzes:
					if len(quiz) < 80 and len(coursename) < 80:
						if len(quiz) != 0:
							
							insert_quiz = ("INSERT IGNORE INTO quizzes (qname, cname) VALUES (%s,%s)")
							curA.execute(insert_quiz,(quiz,coursename))
							cnx.commit()
							if quiz not in quizzes_array:
								quizzes_array.append(quiz)
						#else:
							#print "undefined quizname: *"+quiz+"* for course "+coursename
							#insert_quiz = ("INSERT IGNORE INTO quizzes (qname, cname) VALUES (%s,%s)")
							#curA.execute(insert_quiz,("undefined",coursename))
							#cnx.commit()
					
					
			f.close()

def parseQuizAttempts():
	print "BEGIN PARSING QUIZ ATTEMPTS"
	#Scan quiz attempts and progress and write to DB
	#print quizzes_array
	for file in os.listdir(quizdir):
		
		progress_array = []
		if file.endswith(".scorm"):
			username = "undefined"
			quizname = "undefined"
			for quiz in quizzes_array:
					
				if quiz in file:
					stripstring = "_"+quiz+".scorm"
					username = file.split(stripstring)[0]
					quizname = quiz
					
			destination = quizdir+"/"+str(file)
			f = open(destination, 'r')
			for line in f:
				
				if "progress" in line: #progress not included in first versions
					progress_array = ['0','0','0','0','0']
					progress = line.split("progress")[1]
					progress = progress.replace("%","")
					progress = progress.replace(":","")
					progress = progress.split(";")
					
					i = 0
					#print len(progress)
					for item in progress:
						if i < 5:
							if item == '':
								progress_array[i]='0'
							else: 
								progress_array[i]=item
						i+=1
					
				else: #if progress was not yet included, fill with nulls
					progress_array = ['0','0','0','0','0']
				#print progress_array, username, quizname
				
				
				first =  get_int_secure(progress_array[0])
				second =   get_int_secure(progress_array[1])
				third  = get_int_secure(progress_array[2])
				fourth =  get_int_secure(progress_array[3])
				fifth =  get_int_secure(progress_array[4])
				
				
				if len(quizname) < 80 and len(username) < 80 and quizname != "undefined" and username != "undefined":
					insert_attempt = ("REPLACE INTO quiz_attempts (qname, uname, first, second, third, fourth, fifth) VALUES(%s,%s,%s,%s,%s,%s,%s)")
					
					insert_data = (quizname, username, first, second, third, fourth, fifth)
					curA.execute(insert_attempt, insert_data)
					cnx.commit()
			
			f.close()
	print "END PARSING QUIZ ATTEMPTS"

def parseVideosWatched():
	print "BEGIN PARSING VIDEOS"
	for subdir, dirs, files in os.walk(videodir):
		for dir in dirs:
			#print os.path.join(subdir, file)
			#print dir
			saveCourseName(dir)
			
			destination = videodir+"/"+dir+"/youtube.log.arc"
			
			try:
				f = open(destination, 'r')
			
			
				for line in f:
					if not re.match(r'^\s*$', line) and "[||]"  not in line: # line is empty (has only the following: \t\n\r and whitespace)
						if len(line.split("%:%")) == 6:
							date = line.split("%:%")[0]
							user = line.split("%:%")[1]
							status = line.split("%:%")[2]
							videoid = line.split("%:%")[3]
							watched_time = line.split("%:%")[4]
							total_time = line.split("%:%")[5]
						elif len(line.split("%:%")) == 5:
							date = line.split("%:%")[0]
							user = line.split("%:%")[1]
							status = line.split("%:%")[2]
							videoid = line.split("%:%")[3]
							watched_time = line.split("%:%")[4].split("%;%")[0]
							total_time = line.split("%:%")[4].split("%;%")[1]
						else:
							print "Format of Video Watched not readable"
							
						#print date, dir
						
						try: 
							converted_date = datetime.strptime(' '.join(date.split(" ")[1:5]), '%b %d %Y %H:%M:%S')
							#RESULT: wieviel prozent eines videos hat ein user angeschaut 
							#SELECT uname, vname, MAX(watched_time/total_time * 100) as percent_watched FROM videos_all GROUP BY uname, vname
	
	
							if len(user) < 80:
								insert_attempt = ("REPLACE INTO videos_all (vname, uname, status, watched_time, total_time, date) VALUES(%s,%s,%s,%s,%s,%s)")
								insert_data = (videoid, user, status, watched_time, total_time, converted_date)
								curA.execute(insert_attempt, insert_data)
								insert_video = ("INSERT IGNORE INTO videos (videoid, fullvideoname, cname) VALUES (%s,%s,%s)")
								curA.execute(insert_video,(videoid,videoid,dir))
								
								cnx.commit()
						except ValueError:
                                                	print "Wrong date format: "+date

				
				f.close()
			except IOError:
				print "File "+destination+" not found"
			
	print "END parsing Videos"
	#Delete videos which are in ignored_videos --> the ones that are not online
	delete_video = ("DELETE FROM videos WHERE videoid IN ( SELECT videoid FROM ignored_videos)")
	curA.execute(delete_video)				
	cnx.commit()

def saveCourseName(coursename):	
	if coursename not in ignored_rooms:
		#print coursename
		curA.execute("INSERT IGNORE INTO courses (cname, fullcname) VALUE (%s,%s)", (coursename,coursename))
		courses_array.append(coursename)
		cnx.commit()	


def get_int_secure(s):
    try:
        return int(float(s))
    except ValueError:
        return 0

splunk_data = []

#Deserialization


today = date.today()
day_today = today.strftime('%d')
month_today = today.strftime('%m')
year_today = today.strftime('%Y')

print "today: "+day_today+" "+month_today+" "+year_today

yesterday = date.today() - timedelta(1)
day_yesterday = yesterday.strftime('%d')
month_yesterday = yesterday.strftime('%m')
year_yesterday = yesterday.strftime('%Y')


if len(sys.argv) == 1:
	dayfrom = int(day_yesterday)
	dayto = int(day_yesterday)
	month = month_yesterday
	year = year_yesterday
else:
	dayfrom = int(sys.argv[1])
	dayto = int(sys.argv[2])
	month = sys.argv[3]
	year = sys.argv[4]

if serializeData:
	for i in range(dayfrom-1,dayto):
		if screenOutput and printDeserializationProgress:
			print "File "+str(i+1)+" opened"
		file = open(SERIALIZATION_PATH+"/"+month+"_"+year+"/"+"serialized_"+str(i+1)+"_"+month+"_"+year+".pickle", "rb")
		for _ in range(int(pickle.load(file))):
				splunk_data.append(pickle.load(file))
		if screenOutput and printDeserializationProgress:
			print "File "+str(i+1)+" deserialized"
	
		file.close()
		if screenOutput and printDeserializationProgress:
			print "File "+str(i+1)+" closed"

parseCourseNames()

parseVideosWatched()

parseQuizAttempts()	

previous_uri = None
previous_time_login = None
previous_login_username = None
previous_time_download = None
previous_time_download_username = None
previous_time_forum_read = None
previous_time_forum_read_username = None
previous_time_forum_written = None
previous_time_forum_written_username = None
previous_time_quiz_started = None
previous_time_quiz_started_username = None
previous_time_quiz_ended = None

# iterate over all splunk data chronologically 

if serializeData:
	for result in splunk_data:
		if "_time" in result.keys():
			time = result['_time'].split("+")
			time = time[0]
			#cut the timezone correction, mysql datetime format doesn't like it
		else:
			time = 'not defined'
	
	
		if 'uri' in result.keys() and result['uri'] != previous_uri: #kick simple redundant logs
			#LOGIN
			if "get_user_card" in result['uri'] and 'token' in result.keys() and 'login' in result.keys():
				parseLogin(result,time,previous_time_login,previous_login_username)
				previous_time_login = time
				previous_login_username = result['login']
			#LOGOUT
			if "logout" in result['uri']:
				parseLogout(result,time)
			#QUIZ
			if "method" in result.keys():
				if result['method']=="GET" and ".scorm" in result['uri']:
					parseQuizStarted(result,time,previous_time_quiz_started, previous_time_quiz_started_username)
					previous_time_quiz_started = time
					previous_time_quiz_started_username=filterString(result['uri'],'mainOwp/','_')
				if result['method']=="POST"  and ".scorm" in result['uri']:
					parseQuizEnded(result,time,previous_time_quiz_ended)
					previous_time_quiz_ended = time
			#DOWNLOAD
			if "/wbtmaster/threads" in result['uri'] and 'token' in result.keys():
				parseDownload(result,time,previous_time_download, previous_time_download_username)
				previous_time_download = time
				if result['token'] in logins_tokens.keys():
					previous_time_download_username = logins_tokens[result['token']]
			#FORUM
			if "/wbtmaster/groovy/addForum.groovy?room" in result['uri']  and 'token' in result.keys():	
				parseForumWritten(result,time,previous_time_forum_written, previous_time_forum_written_username)
				previous_time_forum_written = time
				if result['token'] in logins_tokens.keys():
					previous_time_forum_written_username = logins_tokens[result['token']]
			if "/wbtmaster/forum.Forum?action=get&file=" in result['uri'] and 'token' in result.keys():
				parseForumRead(result,time,previous_time_forum_read,previous_time_forum_read_username)
				previous_time_forum_read = time
				if result['token'] in logins_tokens.keys():
					previous_time_forum_read_username = logins_tokens[result['token']]
			previous_uri = result['uri']




	

#save how many times a user has logged in
for key, value in logins_number.iteritems():
	if len(key) < 80:
		insert_stmt = ("REPLACE INTO number_of_logins (uname, times) VALUES(%s,%s)")
		insert_data = (key,value)
		curA.execute(insert_stmt, insert_data)

#TODO: druber nachdenken das commit ueberall anders auch aus den schleifen rauszugeben!!!!!!!
cnx.commit()

curA.close()
cnx.close()

		
#sort dicts
sorted_logins = sorted(logins_number.iteritems(), key=operator.itemgetter(1))
sorted_quizzes = sorted(quizzes_number.iteritems(), key=operator.itemgetter(1))
sorted_quizzes_per_user = sorted(quizzes_per_user.iteritems(), key=operator.itemgetter(1))
sorted_downloads = sorted(downloads_number.iteritems(), key=operator.itemgetter(1))
sorted_downloads_per_user = sorted(downloads_per_user.iteritems(), key=operator.itemgetter(1))
sorted_forum_written = sorted(forum_written.iteritems(), key=operator.itemgetter(1))
sorted_forum_written_per_user = sorted(forum_written_per_user.iteritems(), key=operator.itemgetter(1))
sorted_forum_read = sorted(forum_read.iteritems(), key=operator.itemgetter(1))
sorted_forum_read_per_user = sorted(forum_read_per_user.iteritems(), key=operator.itemgetter(1))

#'/Users/Stephan/Sites/jqueryCharts/examples/logins.txt'
#write2file(sorted_logins,config.get('DEFAULT','basepath')+'/data/logins.txt')
#rite2file(sorted_quizzes,config.get('DEFAULT','basepath')+'/data/quizzes.txt')
#write2file(sorted_quizzes_per_user,config.get('DEFAULT','basepath')+'/data/quizzes_per_user.txt')
#write2file(sorted_downloads,config.get('DEFAULT','basepath')+'/data/downloads.txt')
#write2file(sorted_downloads_per_user,config.get('DEFAULT','basepath')+'/data/downloads_per_user.txt')
#write2file(sorted_forum_written,config.get('DEFAULT','basepath')+'/data/forum_written.txt')
#write2file(sorted_forum_written_per_user,config.get('DEFAULT','basepath')+'/data/forum_written_per_user.txt')
#write2file(sorted_forum_read,config.get('DEFAULT','basepath')+'/data/forum_read.txt')
#write2file(sorted_forum_read_per_user,config.get('DEFAULT','basepath')+'/data/forum_read_per_user.txt')
