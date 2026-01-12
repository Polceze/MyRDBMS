"""Database module for web application using MyRDBMS"""

import sys
import os

# Add parent directory to path to import MyRDBMS
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rdbms.executor import QueryExecutor

def get_db():
    """Get database connection"""
    from flask import current_app, g
    
    if 'db' not in g:
        # Create a new database connection
        db_path = os.path.join(current_app.instance_path, current_app.config['DATABASE'])
        g.db = QueryExecutor(db_path)
    
    return g.db

def close_db(e=None):
    """Close database connection"""
    from flask import g
    db = g.pop('db', None)
    if db is not None:
        pass

def init_app(app):
    """Register database functions with the Flask app"""
    app.teardown_appcontext(close_db)

def init_database():
    """Initialize database with sample tables and data"""
    from flask import current_app
    
    db_path = os.path.join(current_app.instance_path, current_app.config['DATABASE'])
    
    # Remove existing database file
    if os.path.exists(db_path):
        os.remove(db_path)
    
    db = QueryExecutor(db_path)
    
    # Create tables
    print("Creating tables...")
    
    # Students table
    db.execute_raw('''
        CREATE TABLE students (
            student_id INT PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(100) UNIQUE,
            date_of_birth DATE,
            enrollment_year INT
        )
    ''')
    
    # Courses table
    db.execute_raw('''
        CREATE TABLE courses (
            course_id INT PRIMARY KEY,
            course_code VARCHAR(20) UNIQUE NOT NULL,
            course_name VARCHAR(100) NOT NULL,
            instructor VARCHAR(100),
            credits INT
        )
    ''')
    
    # Enrollments table (join table)
    db.execute_raw('''
        CREATE TABLE enrollments (
            enrollment_id INT PRIMARY KEY,
            student_id INT,
            course_id INT,
            enrollment_date DATE,
            grade VARCHAR(2)
        )
    ''')
    
    # Insert sample data
    print("Inserting sample data...")
    
    # Students
    students = [
        (1, 'John', 'Doe', 'john.doe@example.com', '2000-05-15', 2022),
        (2, 'Jane', 'Smith', 'jane.smith@example.com', '2001-08-22', 2022),
        (3, 'Paul', 'Ochieng', 'paul.ochieng@example.com', '1999-12-10', 2021),
        (4, 'Alice', 'Wanjiku', 'alice.wanjiku@example.com', '2000-03-30', 2022),
        (5, 'Will', 'Kilonzo', 'will.kilonzo@example.com', '2002-01-18', 2023)
    ]
    
    for student in students:
        db.execute_raw(f'''
            INSERT INTO students 
            VALUES ({student[0]}, '{student[1]}', '{student[2]}', 
                   '{student[3]}', '{student[4]}', {student[5]})
        ''')
    
    # Courses
    courses = [
        (101, 'CS101', 'Introduction to Computer Science', 'Dr. Timon', 3),
        (102, 'MATH201', 'Calculus I', 'Prof. Betty', 4),
        (103, 'ENG101', 'English Composition', 'Dr. Amollo', 3),
        (104, 'PHYS101', 'Physics Fundamentals', 'Prof. Wilson', 4),
        (105, 'CS201', 'Data Structures', 'Dr. Joyce', 3)
    ]
    
    for course in courses:
        db.execute_raw(f'''
            INSERT INTO courses 
            VALUES ({course[0]}, '{course[1]}', '{course[2]}', 
                   '{course[3]}', {course[4]})
        ''')
    
    # Enrollments
    enrollments = [
        (1, 1, 101, '2023-09-01', 'A'),
        (2, 1, 102, '2023-09-01', 'B+'),
        (3, 2, 101, '2023-09-01', 'A-'),
        (4, 2, 103, '2023-09-01', 'B'),
        (5, 3, 104, '2023-09-01', 'A'),
        (6, 3, 105, '2023-09-01', 'A-'),
        (7, 4, 102, '2023-09-01', 'B+'),
        (8, 4, 104, '2023-09-01', 'A'),
        (9, 5, 103, '2023-09-01', 'B-'),
        (10, 5, 105, '2023-09-01', 'B')
    ]
    
    for enrollment in enrollments:
        db.execute_raw(f'''
            INSERT INTO enrollments 
            VALUES ({enrollment[0]}, {enrollment[1]}, {enrollment[2]}, 
                   '{enrollment[3]}', '{enrollment[4]}')
        ''')
    
    print(f"Database initialized with {len(students)} students, {len(courses)} courses, and {len(enrollments)} enrollments")