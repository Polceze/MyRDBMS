"""Routes for MyRDBMS web application"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from .db import get_db

bp = Blueprint('main', __name__)

@bp.route('/')
def index():
    """Home page - Dashboard"""
    db = get_db()
    
    # Get counts for dashboard
    try:
        students_result = db.execute_raw('SELECT COUNT(*) as student_count FROM students')
        courses_result = db.execute_raw('SELECT COUNT(*) as course_count FROM courses')
        enrollments_result = db.execute_raw('SELECT COUNT(*) as enrollment_count FROM enrollments')
        
        # Extract counts
        student_count = students_result[0]['student_count'] if students_result and len(students_result) > 0 else 0
        course_count = courses_result[0]['course_count'] if courses_result and len(courses_result) > 0 else 0
        enrollment_count = enrollments_result[0]['enrollment_count'] if enrollments_result and len(enrollments_result) > 0 else 0
        
        print(f"Dashboard counts: students={student_count}, courses={course_count}, enrollments={enrollment_count}")
        
    except Exception as e:
        print(f"Error getting dashboard counts: {e}")
        student_count = course_count = enrollment_count = 0
    
    # Get recent enrollments
    recent_enrollments = []
    try:
        recent_enrollments = db.execute_raw('''
            SELECT first_name, last_name, 
                course_name, enrollment_date,
                grade, course_code
            FROM enrollments
            INNER JOIN students ON enrollments.student_id = students.student_id
            INNER JOIN courses ON enrollments.course_id = courses.course_id
            ORDER BY enrollment_date DESC
            LIMIT 5
        ''')
        
        print(f"Dashboard: Found {len(recent_enrollments)} recent enrollments")
        if recent_enrollments and len(recent_enrollments) > 0:
            print(f"First enrollment keys: {list(recent_enrollments[0].keys())}")
            print(f"First enrollment: {recent_enrollments[0]}")
        
    except Exception as e:
        print(f"Error getting recent enrollments: {e}")
    
    return render_template('index.html',
                         student_count=student_count,
                         course_count=course_count,
                         enrollment_count=enrollment_count,
                         recent_enrollments=recent_enrollments)

@bp.route('/students')
def list_students():
    """List all students"""
    db = get_db()
    
    # Get search parameter
    search = request.args.get('search', '')
    
    if search:
        students = db.execute_raw(f'''
            SELECT * FROM students 
            WHERE first_name LIKE '%{search}%' 
               OR last_name LIKE '%{search}%'
               OR email LIKE '%{search}%'
            ORDER BY last_name, first_name
        ''')
    else:
        students = db.execute_raw('SELECT * FROM students ORDER BY last_name, first_name')
    
    return render_template('students/list.html', students=students, search=search)

@bp.route('/students/add', methods=['GET', 'POST'])
def add_student():
    """Add a new student"""
    if request.method == 'POST':
        # Get form data
        student_id = request.form['student_id']
        first_name = request.form['first_name']
        last_name = request.form['last_name']
        email = request.form['email']
        date_of_birth = request.form['date_of_birth']
        enrollment_year = request.form['enrollment_year']
        
        db = get_db()
        
        try:
            db.execute_raw(f'''
                INSERT INTO students 
                VALUES ({student_id}, '{first_name}', '{last_name}', 
                       '{email}', '{date_of_birth}', {enrollment_year})
            ''')
            flash('Student added successfully!', 'success')
            return redirect(url_for('main.list_students'))
        except Exception as e:
            flash(f'Error adding student: {str(e)}', 'danger')
    
    return render_template('students/add.html')

@bp.route('/students/<int:student_id>/edit', methods=['GET', 'POST'])
def edit_student(student_id):
    """Edit a student"""
    db = get_db()
    
    if request.method == 'POST':
        first_name = request.form['first_name']
        last_name = request.form['last_name']
        email = request.form['email']
        date_of_birth = request.form['date_of_birth']
        enrollment_year = request.form['enrollment_year']
        
        try:
            db.execute_raw(f'''
                UPDATE students 
                SET first_name='{first_name}', last_name='{last_name}', 
                    email='{email}', date_of_birth='{date_of_birth}', 
                    enrollment_year={enrollment_year}
                WHERE student_id={student_id}
            ''')
            flash('Student updated successfully!', 'success')
            return redirect(url_for('main.list_students'))
        except Exception as e:
            flash(f'Error updating student: {str(e)}', 'danger')
    
    # GET request - load student data
    student = db.execute_raw(f'SELECT * FROM students WHERE student_id={student_id}')
    
    if not student:
        flash('Student not found', 'danger')
        return redirect(url_for('main.list_students'))
    
    return render_template('students/edit.html', student=student[0])

@bp.route('/students/<int:student_id>/delete')
def delete_student(student_id):
    """Delete a student"""
    db = get_db()
    
    try:
        db.execute_raw(f'DELETE FROM students WHERE student_id={student_id}')
        flash('Student deleted successfully!', 'success')
    except Exception as e:
        flash(f'Error deleting student: {str(e)}', 'danger')
    
    return redirect(url_for('main.list_students'))

@bp.route('/courses')
def list_courses():
    """List all courses"""
    db = get_db()
    
    search = request.args.get('search', '')
    
    if search:
        courses = db.execute_raw(f'''
            SELECT * FROM courses 
            WHERE course_name LIKE '%{search}%' 
               OR course_code LIKE '%{search}%'
               OR instructor LIKE '%{search}%'
            ORDER BY course_code
        ''')
    else:
        courses = db.execute_raw('SELECT * FROM courses ORDER BY course_code')
    
    return render_template('courses/list.html', courses=courses, search=search)

@bp.route('/enrollments')
def list_enrollments():
    """List all enrollments with JOIN"""
    db = get_db()
    
    try:
        enrollments = db.execute_raw('''
            SELECT enrollment_id, enrollment_date, grade,
                   student_id, first_name, last_name,
                   course_id, course_code, course_name
            FROM enrollments
            INNER JOIN students ON enrollments.student_id = students.student_id
            INNER JOIN courses ON enrollments.course_id = courses.course_id
            ORDER BY enrollment_date DESC
        ''')
        print(f"Enrollments page: Found {len(enrollments)} enrollments")
        if enrollments and len(enrollments) > 0:
            print(f"First enrollment keys: {list(enrollments[0].keys())}")
    except Exception as e:
        print(f"Error loading enrollments: {e}")
        enrollments = []
    
    return render_template('enrollments/list.html', enrollments=enrollments)

@bp.route('/query', methods=['GET', 'POST'])
def sql_query():
    """Execute raw SQL queries"""
    if request.method == 'POST':
        sql = request.form.get('sql', '')
        db = get_db()
        
        try:
            result = db.execute_raw(sql)
            return render_template('query.html', sql=sql, result=result, error=None)
        except Exception as e:
            return render_template('query.html', sql=sql, result=None, error=str(e))
    
    return render_template('query.html', sql='', result=None, error=None)

@bp.route('/api/query', methods=['POST'])
def api_query():
    """API endpoint for executing SQL queries"""
    data = request.get_json()
    sql = data.get('sql', '')
    
    if not sql:
        return jsonify({'error': 'No SQL query provided'}), 400
    
    db = get_db()
    
    try:
        result = db.execute_raw(sql)
        return jsonify({'success': True, 'result': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@bp.route('/demo')
def demo():
    """Demonstration page showing MyRDBMS features"""
    db = get_db()
    
    # Demonstrate different SQL features
    demo_queries = [
        {
            'title': 'Simple SELECT',
            'sql': 'SELECT * FROM students LIMIT 3',
            'result': db.execute_raw('SELECT * FROM students LIMIT 3')
        },
        {
            'title': 'SELECT with WHERE',
            'sql': "SELECT * FROM students WHERE enrollment_year = 2022",
            'result': db.execute_raw("SELECT * FROM students WHERE enrollment_year = 2022")
        },
        {
            'title': 'INNER JOIN',
            'sql': '''SELECT students.first_name, students.last_name, 
                     courses.course_name, enrollments.grade
                     FROM enrollments
                     INNER JOIN students ON enrollments.student_id = students.student_id
                     INNER JOIN courses ON enrollments.course_id = courses.course_id
                     LIMIT 3''',
            'result': db.execute_raw('''SELECT students.first_name, students.last_name, 
                     courses.course_name, enrollments.grade
                     FROM enrollments
                     INNER JOIN students ON enrollments.student_id = students.student_id
                     INNER JOIN courses ON enrollments.course_id = courses.course_id
                     LIMIT 3''')
        },
        {
            'title': 'CREATE INDEX (if not exists)',
            'sql': 'CREATE INDEX idx_email ON students(email)',
            'result': 'Index created (or already exists)'
        }
    ]
    
    return render_template('demo.html', demo_queries=demo_queries)