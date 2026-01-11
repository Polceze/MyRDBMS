"""Main entry point for MyRDBMS CLI"""

from rdbms.repl import REPL

def main():
    """Main function"""
    repl = REPL()
    repl.start()

if __name__ == "__main__":
    main()