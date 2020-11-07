import mysql.connector

class EnrollDB:
    """Application for an enrollment database on MySQL"""

    def connect(self):
        """Makes a connection to the database and returns connection to caller"""
        try:
            # TODO: Fill in your connection information
            print("Connecting to database.")
            self.cnx = mysql.connector.connect(user='epinno', password='13508460', host='cosc304.ok.ubc.ca', database='db_epinno')
            return self.cnx
        except mysql.connector.Error as err:  
            print(err)   

    def init(self):
        """Creates and initializes the database"""
        fileName = "university.ddl"
        print("Loading data")
        
        try:
            cursor = self.cnx.cursor()
            with open(fileName, "r") as infile:
                st = infile.read()
                commands = st.split(";")
                for line in commands:                   
                    # print(line.strip("\n"))
                    line = line.strip()
                    if line == "":  # Skip blank lines
                        continue 
                        
                    cursor.execute(line)                    
                        
            cursor.close()
            self.cnx.commit()            
            print("Database load complete")
        except mysql.connector.Error as err:  
            print(err)
            self.cnx.rollback()  
        
    def close(self):
        try:
            print("Closing database connection.")
            self.cnx.close()
        except mysql.connector.Error as err:  
            print(err)   

    def listAllStudents(self):
        """ Returns a String with all the students in the database.  
            Format:
                sid, sname, sex, birthdate, gpa
                00005465, Joe Smith, M, 1997-05-01, 3.20

            Return:
                String containing all student information"""
        
        output = "sid, sname, sex, birthdate, gpa"
        cursor = self.cnx.cursor()
        # TODO: Execute query and build output string                
        sql = """ SELECT * 
        FROM student;
        """
        cursor.execute(sql)
        for row in cursor:
            row = list(row) # Convert from tuple
            row = [str(i) for i in row]
            output += "\n" + ", ".join(row)

        cursor.close()
        
        return output

    def listDeptProfessors(self, deptName):
        """Returns a String with all the professors in a given department name.
           Format:
                    Professor Name, Department Name
                    Art Funk, Computer Science
           Returns:
                    String containing professor information"""

        # TODO: Execute query and build output string 
        output = "Professor Name, Department Name"
        cursor = self.cnx.cursor()
        sql = """ SELECT prof.pname,prof.dname
        FROM prof
        WHERE prof.dname = %s;
        """
        cursor.execute(sql,(deptName,))
        for row in cursor:
            row = list(row) # Convert from tuple
            row = [str(i) for i in row]
            output += "\n" + ", ".join(row)
        
        cursor.close()
        return output

    def listCourseStudents(self, courseNum):
        if courseNum == None:
            return "Student Id, Student Name, Course Number, Section Number"
        """Returns a String with all students in a given course number (all sections).
            Format:
                Student Id, Student Name, Course Number, Section Number
                00005465, Joe Smith, COSC 304, 001
            Return:
                 String containing students"""

        # TODO: Execute query and build output string 
        output = "Student Id, Student Name, Course Number, Section Number"
        cursor = self.cnx.cursor()
        sql = """SELECT student.sid, student.sname, enroll.cnum, enroll.secnum
        FROM student JOIN enroll ON student.sid = enroll.sid
        WHERE enroll.cnum = %s;
        """
        cursor.execute(sql,(courseNum,))
        for row in cursor:
            row = list(row) # Convert from tuple
            row = [str(i) for i in row]
            output += "\n" + ", ".join(row)
        
        cursor.close()
        return output

    def computeGPA(self, studentId):
        """Returns a cursor with a row containing the computed GPA (named as gpa) for a given student id."""

        # TODO: Execute the query and return a cursor
        cursor = self.cnx.cursor()
        sql = """ SELECT AVG(grade) as gpa
        FROM enroll
        WHERE enroll.sid = %s
        """
        cursor.execute(sql,(studentId,))
        return cursor

    def addStudent(self, studentId, studentName, sex, birthDate):
        """Inserts a student into the databases."""

        # TODO: Execute statement. Make sure to commit
        cursor = self.cnx.cursor()
        sql = """ INSERT INTO student(sid,sname,sex,birthdate) 
        VALUES(%s, %s, %s, %s)
        """
        cursor.execute(sql,(studentId,studentName,sex,birthDate))
        cursor.close()
        self.cnx.commit()
        return
    
    def deleteStudent(self, studentId):
        """Deletes a student from the databases."""

        # TODO: Execute statement. Make sure to commit
        cursor = self.cnx.cursor()
        sql = """ DELETE FROM student 
        WHERE student.sid = %s
        """
        cursor.execute(sql,(studentId,))
        cursor.close()
        self.cnx.commit()
        return

    def updateStudent(self, studentId, studentName, sex, birthDate, gpa):
        """Updates a student in the databases."""

        # TODO: Execute statement. Make sure to commit
        cursor = self.cnx.cursor()
        sql = """UPDATE student
        SET sname = %s, sex = %s, birthdate = %s, gpa = %s
        WHERE student.sid = %s
        """
        cursor.execute(sql,(studentName,sex,birthDate,gpa,studentId))
        cursor.close()
        self.cnx.commit()
        return
        
    def newEnroll(self, studentId, courseNum, sectionNum, grade):
        """Creates a new enrollment in a course section."""
        
        # TODO: Execute statement. Make sure to commit
        cursor = self.cnx.cursor()
        sql = """ INSERT INTO enroll(sid,cnum,secnum,grade) 
        VALUES(%s, %s, %s, %s)
        """
        cursor.execute(sql,(studentId,courseNum,sectionNum,grade))
        cursor.close()
        self.cnx.commit()
        return

    def updateStudentGPA(self, studentId):
        """ Updates a student's GPA based on courses taken."""

        # TODO: Execute statement. Make sure to commit
        updateCursor = self.computeGPA(studentId)
        avg = 0 # Just in case there are no courses being taken - default average of 0
        for row in updateCursor: # Should only be one row
            avg = row[0]
        updateCursor.close()
        # TODO: Execute statement. Make sure to commit
        cursor = self.cnx.cursor()
        sql = """UPDATE student
        SET gpa = %s
        WHERE student.sid = %s
        """
        cursor.execute(sql,(avg,studentId))
        cursor.close()
        self.cnx.commit()

        return

    def removeStudentFromSection(self, studentId, courseNum, sectionNum):
        """Removes a student from a course and updates their GPA."""

        # TODO: Execute statement. Make sure to commit
        cursor = self.cnx.cursor()
        sql = """ DELETE FROM enroll 
        WHERE enroll.sid = %s AND cnum = %s AND secnum = %s
        """
        cursor.execute(sql,(studentId,courseNum,sectionNum))
        cursor.close()
        self.cnx.commit()
        self.updateStudentGPA(studentId) # Update overall GPA
        return

    def updateStudentMark(self, studentId, courseNum, sectionNum, grade):
        """Updates a student's mark in an enrolled course section and updates their grade."""

        # TODO: Execute statement. Make sure to commit
        cursor = self.cnx.cursor()
        sql = """ UPDATE enroll
        SET grade = %s
        WHERE enroll.sid = %s AND enroll.cnum = %s AND enroll.secnum = %s
        """
        cursor.execute(sql,(grade,studentId,courseNum,sectionNum))
        cursor.close()
        self.cnx.commit()
        self.updateStudentGPA(studentId) # Update overall GPA
        return

    def query1(self):
        """Return the list of students (id and name) that have not registered in any course section. Hint: Left join can be used instead of a subquery."""

        sql = """SELECT S.sid, sname
        FROM student AS S LEFT JOIN enroll AS E ON S.sid = E.sid
        WHERE E.sid IS NULL
        """
        cursor = self.cnx.cursor()
        cursor.execute(sql)
        return cursor

    def query2(self):
        """For each student return their id and name, number of course sections registered in (called numcourses), and gpa (average of grades). 
        Return only students born after March 15, 1992. A student is also only in the result if their gpa is above 3.1 or registered in 0 courses.
        Order by GPA descending then student name ascending and show only the top 5."""

         # TODO: Execute the query and return a cursor
        sql = """SELECT S.sid, sname, COUNT(cnum) AS numcourses, AVG(grade) AS  gpa
        FROM student AS S LEFT JOIN enroll AS E ON S.sid = E.sid
        WHERE (E.sid IS NULL OR gpa > 3.1) AND birthdate > date('1992-03-15')
        GROUP BY S.sid
        ORDER BY gpa DESC, sname ASC
        LIMIT 5
        """
        cursor = self.cnx.cursor()
        cursor.execute(sql)
        return cursor

    def query3(self):
        """For each course, return the number of sections (numsections), total number of students enrolled (numstudents), average grade (avggrade), and number of distinct professors who taught the course (numprofs).
            Only show courses in Chemistry or Computer Science department. Make sure to show courses even if they have no students. Do not show a course if there are no professors teaching that course. 
            Format:
            cnum, numsections, numstudents, avggrade, numprof"""

         # TODO: Execute the query and return a cursor
        sql = """SELECT S.cnum, COUNT(DISTINCT (S.secnum)) AS numsections, COUNT(sid) AS numstudents, AVG(grade) AS avggrade, COUNT(DISTINCT (pname)) AS numprofs
        FROM section AS S LEFT JOIN enroll AS E ON S.cnum = E.cnum and S.secnum = E.secnum
        WHERE (S.cnum LIKE 'CHEM%' OR S.cnum LIKE 'COSC%') AND pname IS NOT NULL
        GROUP BY S.cnum
        """
        cursor = self.cnx.cursor()
        cursor.execute(sql)
        return cursor

    def query4(self):
        """Return the students who received a higher grade than their course section average in at least two courses. Order by number of courses higher than the average and only show top 5.
            Format:
            sid, sname, numhigher"""

        # TODO: Execute the query and return a cursor
        sql = """SELECT S.sid, sname, count(grade) as numhigher
        FROM student AS S JOIN enroll AS E ON S.sid = E.sid
        WHERE E.grade > (SELECT AVG(grade) FROM enroll E2 WHERE E2.cnum=E.cnum AND E2.secnum=E.secnum)
        GROUP BY S.sid
        HAVING numhigher > 1
        ORDER BY count(grade) DESC, sname ASC
        LIMIT 5
        """
        cursor = self.cnx.cursor()
        cursor.execute(sql)
        return cursor


# Do NOT change anything below here
    def resultSetToString(self, cursor, maxrows):
        if cursor == None:
            return None
        output = ""
        cols = cursor.column_names
        output += "Total columns: "+str(len(cols))+"\n"
        output += str(cols[0])
        for i in range(1,len(cols)):
            output += ", "+str(cols[i])
        for row in cursor:
            output += "\n"+str(row[0])
            for i in range(1,len(cols)):
                output += ", "+str(row[i])
        output += "\nTotal results: "+str(cursor.rowcount)
        return output
               
# Main execution for testing
x="""
enrollDB = EnrollDB()
enrollDB.connect()
enrollDB.init()

print(enrollDB.listAllStudents())

print("Executing list professors in a department: Computer Science")
print(enrollDB.listDeptProfessors("Computer Science"))
print("Executing list professors in a department: none")
print(enrollDB.listDeptProfessors("none"))

print("Executing list students in course: COSC 304")
enrollDB.listCourseStudents("COSC 304")
print("Executing list students in course: DATA 301")
enrollDB.listCourseStudents("DATA 301")


print("Executing compute GPA for student: 45671234")
print(enrollDB.resultSetToString(enrollDB.computeGPA("45671234"),10))
print("Executing compute GPA for student: 00000000")
enrollDB.resultSetToString(enrollDB.computeGPA("00000000"),10)

print("Adding student 55555555:")
enrollDB.addStudent("55555555",  "Stacy Smith", "F", "1998-01-01")
print("Adding student 11223344:")
enrollDB.addStudent("11223344",  "Jim Jones", "M",  "1997-12-31")
print(enrollDB.listAllStudents())

print("Test delete student:");
print("Deleting student 99999999:")
enrollDB.deleteStudent("99999999")
# Non-existing student
print("Deleting student 00000000:")
enrollDB.deleteStudent("00000000")
print(enrollDB.listAllStudents())

print("Updating student 99999999:")
enrollDB.updateStudent("99999999",  "Wang Wong", "F", "1995-11-08", 3.23)
print("Updating student 00567454:")
enrollDB.updateStudent("00567454",  "Scott Brown", "M",  None, 4.00)
print(enrollDB.listAllStudents())

print("Test new enrollment in COSC 304 for 98123434:")
enrollDB.newEnroll("98123434", "COSC 304", "001", 2.51)

enrollDB.init()
print("Test update student GPA for student:")
enrollDB.newEnroll("98123434", "COSC 304", "001", 3.97);  
enrollDB.updateStudentGPA("98123434")

print("Test update student mark for student 98123434 to 3.55:")
enrollDB.updateStudentMark("98123434", "COSC 304", "001", 3.55)

enrollDB.init()

enrollDB.removeStudentFromSection("00546343", "CHEM 113", "002")

# Queries
# Re-initialize all data
enrollDB.init()
print(enrollDB.resultSetToString(enrollDB.query1(), 100))
print(enrollDB.resultSetToString(enrollDB.query2(), 100))
print(enrollDB.resultSetToString(enrollDB.query3(), 100))
print(enrollDB.resultSetToString(enrollDB.query4(), 100))
        
enrollDB.close()
"""