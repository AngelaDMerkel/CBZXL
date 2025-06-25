import sqlite3
import argparse
import os
from rich.console import Console

# --- Constants ---
# This should match the constant in your main processing script
DB_FILE = "converted_archives.db"

# --- Initial Setup ---
console = Console()

def migrate_database(db_path):
    """
    Checks for and adds the 'dominant_type' column to the 'converted_archives' table.
    """
    if not os.path.exists(db_path):
        console.print(f"[red]Error: Database file not found at '{db_path}'. Cannot migrate.[/red]")
        return

    console.print(f"Attempting to migrate database: [cyan]{db_path}[/cyan]")

    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 1. Check if the column already exists
        cursor.execute("PRAGMA table_info(converted_archives)")
        columns = [row[1] for row in cursor.fetchall()]

        if 'dominant_type' in columns:
            console.print("[green]Column 'dominant_type' already exists. No migration needed.[/green]")
        else:
            # 2. If it doesn't exist, add it
            console.print("[yellow]Column 'dominant_type' not found. Adding it now...[/yellow]")
            cursor.execute("ALTER TABLE converted_archives ADD COLUMN dominant_type TEXT")
            conn.commit()
            console.print("[bold green]Migration successful! The 'dominant_type' column has been added.[/bold green]")

    except sqlite3.Error as e:
        console.print(f"[red]A database error occurred: {e}[/red]")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            console.print("Database connection closed.")


def main():
    """Main function to run the migration."""
    parser = argparse.ArgumentParser(
        description="Migration script to add the 'dominant_type' column to an existing cbzxl database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "db_file",
        nargs="?",
        default=DB_FILE,
        help="Path to the database file to migrate."
    )
    args = parser.parse_args()

    migrate_database(args.db_file)


if __name__ == "__main__":
    main()