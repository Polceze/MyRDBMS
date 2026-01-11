"""Web application entry point for MyRDBMS"""

from web_app import create_app

app = create_app()

if __name__ == '__main__':
    print("Starting SimpleRDBMS Web Interface...")
    print("Visit: http://localhost:5000")
    print("Press Ctrl+C to stop")
    app.run(debug=True, host='0.0.0.0', port=5000)