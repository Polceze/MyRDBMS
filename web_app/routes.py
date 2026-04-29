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
        
        # Extract counts — handle both aliased and unaliased keys
        def extract_count(result, alias):
            if not result:
                return 0
            row = result[0]
            return row.get(alias) or row.get('COUNT(*)') or 0

        student_count = extract_count(students_result, 'student_count')
        course_count = extract_count(courses_result, 'course_count')
        enrollment_count = extract_count(enrollments_result, 'enrollment_count')
        
    except Exception as e:
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
    except Exception:
        recent_enrollments = []
    
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
    """Delete a student and their associated enrollments"""
    db = get_db()

    try:
        # Delete associated enrollments first (no FK cascade in custom RDBMS)
        db.execute_raw(f'DELETE FROM enrollments WHERE student_id={student_id}')
        db.execute_raw(f'DELETE FROM students WHERE student_id={student_id}')
        flash('Student and their enrollments deleted successfully!', 'success')
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
    except Exception:
        enrollments = []

    # Load students and courses for the enroll form
    try:
        students = db.execute_raw('SELECT student_id, first_name, last_name FROM students ORDER BY last_name, first_name')
    except Exception:
        students = []

    try:
        courses = db.execute_raw('SELECT course_id, course_code, course_name FROM courses ORDER BY course_code')
    except Exception:
        courses = []

    return render_template('enrollments/list.html',
                           enrollments=enrollments,
                           students=students,
                           courses=courses)


@bp.route('/enrollments/add', methods=['POST'])
def add_enrollment():
    """Enroll a student in a course"""
    db = get_db()

    student_id = request.form.get('student_id')
    course_id = request.form.get('course_id')
    enrollment_date = request.form.get('enrollment_date')
    grade = request.form.get('grade', '').strip()

    if not student_id or not course_id or not enrollment_date:
        flash('Student, course, and enrollment date are required.', 'danger')
        return redirect(url_for('main.list_enrollments'))

    try:
        # Get next enrollment ID safely (MAX + 1 to avoid collisions after deletes)
        id_result = db.execute_raw('SELECT MAX(enrollment_id) FROM enrollments')
        max_id = int(id_result[0].get('MAX(enrollment_id)') or 0)
        next_id = max_id + 1

        # Check for duplicate enrollment
        existing = db.execute_raw(f'''
            SELECT enrollment_id FROM enrollments
            WHERE student_id={student_id} AND course_id={course_id}
        ''')
        if existing:
            flash('This student is already enrolled in that course.', 'warning')
            return redirect(url_for('main.list_enrollments'))

        grade_val = 'NULL' if not grade else f"'{grade}'"
        db.execute_raw(f'''
            INSERT INTO enrollments
            VALUES ({next_id}, {student_id}, {course_id}, '{enrollment_date}', {grade_val})
        ''')
        flash('Enrollment added successfully!', 'success')
    except Exception as e:
        flash(f'Error adding enrollment: {str(e)}', 'danger')

    return redirect(url_for('main.list_enrollments'))


@bp.route('/enrollments/<int:enrollment_id>/delete')
def delete_enrollment(enrollment_id):
    """Delete an enrollment"""
    db = get_db()

    try:
        db.execute_raw(f'DELETE FROM enrollments WHERE enrollment_id={enrollment_id}')
        flash('Enrollment removed successfully.', 'success')
    except Exception as e:
        flash(f'Error removing enrollment: {str(e)}', 'danger')

    return redirect(url_for('main.list_enrollments'))

@bp.route('/enrollments/<int:enrollment_id>/grade', methods=['POST'])
def update_grade(enrollment_id):
    """Update the grade for an enrollment"""
    db = get_db()
    grade = request.form.get('grade', '').strip()

    try:
        grade_val = f"'{grade}'" if grade else 'NULL'
        db.execute_raw(f"UPDATE enrollments SET grade={grade_val} WHERE enrollment_id={enrollment_id}")
        flash('Grade updated.', 'success')
    except Exception as e:
        flash(f'Error updating grade: {str(e)}', 'danger')

    return redirect(url_for('main.list_enrollments'))


@bp.route('/courses/add', methods=['GET', 'POST'])
def add_course():
    """Add a new course"""
    if request.method == 'POST':
        course_id = request.form['course_id']
        course_code = request.form['course_code']
        course_name = request.form['course_name']
        instructor = request.form.get('instructor', '')
        credits = request.form.get('credits', 3)

        db = get_db()
        try:
            db.execute_raw(f'''
                INSERT INTO courses
                VALUES ({course_id}, '{course_code}', '{course_name}',
                        '{instructor}', {credits})
            ''')
            flash('Course added successfully!', 'success')
            return redirect(url_for('main.list_courses'))
        except Exception as e:
            flash(f'Error adding course: {str(e)}', 'danger')

    return render_template('courses/add.html')


@bp.route('/courses/<int:course_id>/delete')
def delete_course(course_id):
    """Delete a course"""
    db = get_db()
    try:
        db.execute_raw(f'DELETE FROM courses WHERE course_id={course_id}')
        flash('Course deleted successfully!', 'success')
    except Exception as e:
        flash(f'Error deleting course: {str(e)}', 'danger')
    return redirect(url_for('main.list_courses'))


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