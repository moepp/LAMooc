from time import sleep
import sys
import pprint, pickle
import operator
import mysql.connector
import os


#screen Output options
screenOutput = False

printLogins = False
printLogouts = False
printQuizzesStarted = False
printQuizzesEnded = False
printDownloads = False
printForumRead = True
printForumWritten = True

printDeserializationProgress = True



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
cnx = mysql.connector.connect(user='stephan',password='123456', database='test_db', host='localhost')
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

def parseLogin(result,time):
        if "uri" in result.keys() and "token" in result.keys():
                if "get_user_card" in result['uri']:
                        if screenOutput and printLogins:
				print "LOGIN by          ",result['login'],"                     at ", time
                        logins_tokens[result['token']] = result['login']
			insert_new_user = ("REPLACE INTO users  (uname, token, last_time) VALUES (%s,%s,%s)")
			curA.execute(insert_new_user,( result['login'],result['token'],time))
        		cnx.commit()
			if result['login'] in  logins_number:
                		logins_number[result['login']] += 1
			else:
				logins_number[result['login']] = 1
		if "logout" in result['uri']:
			if screenOutput and printLogouts:
                        	if result['token'] in logins_tokens.keys():
                                	print "LOGOUT by         ", logins_tokens[result['token']]," at ",time
                       		else:
                                	print "LOGOUT by         ", result['token'], " at ", time
	
def parseQuiz(result,time):
        if "method" in result.keys():
                if result['method']=="GET" and ".scorm" in result['uri']:
                        username=filterString(result['uri'],'mainOwp/','_')
                        quizname=filterString(result['uri'], username+'_', '.scorm')
			coursename=quizname.split("_")[0]
			
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

			
			insert_quiz_started = ("INSERT INTO quizzes_started (qname, uname, time) VALUES (%s,%s,%s)")
			curA.execute(insert_quiz_started,(quizname,username,time))
        		cnx.commit()
                if result['method']=="POST"  and ".scorm" in result['uri']:
                        username=filterString(result['uri'],'mainOwp/','_')
                        quizname=filterString(result['uri'], username+'_', '.scorm')
                        if screenOutput and printQuizzesEnded:
				print "QUIZ         ", quizname, "ended by ", username," at ", time

def parseDownload(result,time):
	if "/wbtmaster/threads" in result['uri']:
		if "token" in result.keys() and result['token'] in logins_tokens.keys():
			filename = 'unknown'
			for course in courses_array:
				course_adapted = course + "/"
				if course_adapted in result['uri']:
					filename=filterString(result['uri'],course_adapted,'?token')
					curA.execute("INSERT IGNORE INTO files (fname,cname) VALUES (%s,%s)",(filename,course))
        				cnx.commit()
					curA.execute("INSERT INTO files_downloaded (fname,uname,time) VALUES (%s,%s,%s)",(filename,logins_tokens[result['token']],time))
        				cnx.commit()	
			print filename

			if screenOutput and printDownloads:
	                	print "DOWNLOAD of file ",filename, "by ",logins_tokens[result['token']]
			if filename in downloads_number:
				downloads_number[filename] +=1
			else: 
				downloads_number[filename] = 1

			if logins_tokens[result['token']] in downloads_per_user:
				downloads_per_user[logins_tokens[result['token']]] += 1
			else:
				downloads_per_user[logins_tokens[result['token']]] = 1

def parseForum(result,time):

        if "/wbtmaster/groovy/addForum.groovy?room" in result['uri']  and 'token' in result.keys():
                #print result['uri']
		if "token" in result.keys() and result['token'] in logins_tokens.keys():
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
			if forumname in courses_array:
				curA.execute("INSERT INTO forums_written (fname,uname,time) VALUES (%s,%s,%s)",(forumname,logins_tokens[result['token']],time))
        			cnx.commit()
			else:
				print room
	
			
        if "/wbtmaster/forum.Forum?action=get&file=" in result['uri'] and 'token' in result.keys():
                room=filterString(result['uri'],"file=","/")
                if result['token'] in logins_tokens.keys() and room not in ignored_rooms:
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
				curA.execute("INSERT IGNORE INTO forums (fname,cname) VALUES (%s,%s)",(room,room))
        			cnx.commit()
				curA.execute("INSERT INTO forums_read (fname,uname,time) VALUES (%s,%s,%s)",(room,logins_tokens[result['token']],time))
        			cnx.commit()
			else: 
				print room

splunk_data = []

#Deserialization
for i in range(0):
	if screenOutput and printDeserializationProgress:
		print "Opened file "+str(i+1)
	file = open("serialized_splunk_data/serialized_"+str(i+1)+"_04_2014.pickle", "rb")
    	#print type(pickle.load(f))
	for _ in range(int(pickle.load(file))):
       		splunk_data.append(pickle.load(file))
	if screenOutput and printDeserializationProgress:
		print "File "+str(i+1)+" deserialized"

	file.close()
	if screenOutput and printDeserializationProgress:
		print "File "+str(i+1)+" closed"

#Scan existing quizzes and existing courses
for file in os.listdir("quizzes"):
	if file.endswith(".txt"):
		coursename = file.split("_")[0]
		curA.execute("INSERT IGNORE INTO courses (cname, fullcname) VALUE (%s,%s)", (coursename,coursename))
		courses_array.append(coursename)
		destination = "quizzes/"+str(file)
		f = open(destination, 'r')
		for line in f:
			quizzes = line.split(";")
			for quiz in quizzes:
				insert_quiz = ("INSERT IGNORE INTO quizzes (qname, cname) VALUES (%s,%s)")
				curA.execute(insert_quiz,(quiz,coursename))
				cnx.commit()
				quizzes_array.append(quiz)
		f.close()

for file in os.listdir("quizzes"):
	#print quizzes
	progress_array = []
	if file.endswith(".scorm"):
		#print file
		for quiz in quizzes_array:
			if quiz in file:
				stripstring = "_"+quiz+".scorm"
				username = file.split(stripstring)[0]
				quizname = quiz
				
		destination = "quizzes/"+str(file)
		f = open(destination, 'r')
		for line in f:
			
			if "progress" in line:
				progress_array = ['0','0','0','0','0']
				progress = line.split("progress")[1]
				progress = progress.replace("%","")
				progress = progress.replace(":","")
				progress = progress.split(";")
				#print progress
				i = 0
				#print len(progress)
				for item in progress:
					if i < 5:
						if item == '':
							progress_array[i]='0'
						else: 
							progress_array[i]=item
					#if i <= 5:
					#	if item == '':
					#		progress_array.append('0')
					#	else:
					#		progress_array.append(item)
					i+=1
				
			else:
				progress_array = ['0','0','0','0','0']
			#print progress_array, username, quizname
			first =  int(float(progress_array[0]))
			second =   int(float(progress_array[1]))
			third  =  int(float(progress_array[2]))
			fourth =  int(float(progress_array[3]))
			fifth =  int(float(progress_array[4]))

			insert_attempt = ("REPLACE INTO quiz_attempts (qname, uname, first, second, third, fourth, fifth) VALUES(%s,%s,%s,%s,%s,%s,%s)")
			insert_data = (quizname, username, first, second, third, fourth, fifth)
			curA.execute(insert_attempt, insert_data)
			cnx.commit()
		
		f.close()
			

previous_uri = None


# iterate over all splunk data chronologically 
for result in splunk_data:
	if "_time" in result.keys():
        	time = result['_time'].split("+")
		time = time[0]
		#cut the timezone correction, mysql datetime format doesn't like it
    	else:
        	time = 'not defined'


	if 'uri' in result.keys() and result['uri'] != previous_uri: #kick simple redundant logs
		#print result['uri']
		parseLogin(result,time)
		parseQuiz(result,time)
		parseDownload(result,time)
		parseForum(result,time)
		previous_uri = result['uri']
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
write2file(sorted_logins,'data/logins.txt')
write2file(sorted_quizzes,'data/quizzes.txt')
write2file(sorted_quizzes_per_user,'data/quizzes_per_user.txt')
write2file(sorted_downloads,'data/downloads.txt')
write2file(sorted_downloads_per_user,'data/downloads_per_user.txt')
write2file(sorted_forum_written,'data/forum_written.txt')
write2file(sorted_forum_written_per_user,'data/forum_written_per_user.txt')
write2file(sorted_forum_read,'data/forum_read.txt')
write2file(sorted_forum_read_per_user,'data/forum_read_per_user.txt')
