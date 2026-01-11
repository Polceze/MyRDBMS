"""Flask app factory for MyRDBMS web interface"""

from flask import Flask
import os

def create_app(test_config=None):
    """Create and configure the Flask application"""
    app = Flask(__name__, instance_relative_config=True)
    
    # Default configuration
    app.config.from_mapping(
        SECRET_KEY='dev-secret-key-for-simple-rdbms',
        DATABASE='web_database.db',
    )
    
    # Ensure instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass
    
    # Initialize database
    from . import db
    db.init_app(app)
    
    # Register blueprints
    from . import routes
    app.register_blueprint(routes.bp)
    
    # Add CLI commands
    @app.cli.command('init-db')
    def init_db_command():
        """Initialize the database with sample data"""
        db.init_database()
        print('Initialized the database.')
    
    return app