import sqlite3
import os
from rich.console import Console
from rich.table import Table
from enum import Enum

# Fixed Constants (should match the original script)
DB_FILE = "converted_archives.db"
FAILED_DB_FILE = "failed_archives.db"

console = Console()

class ConversionStatus(Enum):
    PROCESSED_SAVED_SPACE = 1
    PROCESSED_NO_SPACE_SAVED = 2
    ALREADY_JXL_NO_CONVERTIBLES = 3
    NO_JPG_PNG_FOUND = 4
    NO_IMAGES_RECOGNIZED = 5
    CONTAIN_OTHER_FORMATS = 6

def get_db_connection(db_path):
    """Establishes a connection to the SQLite database."""
    if not os.path.exists(db_path):
        console.print(f"[red]Error: Database file not found at '{db_path}'[/red]")
        return None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row # Allows access to columns by name
        return conn
    except sqlite3.Error as e:
        console.print(f"[red]Error connecting to database '{db_path}': {e}[/red]")
        return None

def analyze_conversion_data():
    console.print(f"\n[bold green]--- Analyzing Conversion Data from {DB_FILE} ---[/bold green]")

    conn = get_db_connection(DB_FILE)
    if not conn:
        return

    try:
        # 1. Get and display table fields
        cursor = conn.execute("PRAGMA table_info(converted_archives)")
        fields = [row['name'] for row in cursor.fetchall()]
        console.print("\n[bold blue]Fields in 'converted_archives' table:[/bold blue]")
        for field in fields:
            console.print(f"- {field}")

        # 2. Query for data analysis
        query = "SELECT path, original_size, final_size, bytes_saved, percent_saved, status FROM converted_archives"
        data = conn.execute(query).fetchall()

        if not data:
            console.print("\n[yellow]No conversion data found in the database.[/yellow]")
            return

        total_bytes_saved_overall = 0
        total_bytes_saved_from_converted = 0
        jxl_archives_count = 0
        other_image_type_archives_count = 0
        total_processed_archives = 0

        # Detailed breakdown of archive types
        status_counts = {status.name: 0 for status in ConversionStatus}
        status_counts['failed'] = 0 # Include failed archives
        status_counts['unknown_status'] = 0 # For any status not explicitly handled by enum

        for row in data:
            status = row['status']
            bytes_saved = row['bytes_saved'] if row['bytes_saved'] is not None else 0
            
            total_bytes_saved_overall += bytes_saved

            if status == 'processed':
                total_processed_archives += 1
                if bytes_saved > 0:
                    total_bytes_saved_from_converted += bytes_saved
            
            if status == ConversionStatus.ALREADY_JXL_NO_CONVERTIBLES.name:
                jxl_archives_count += 1
                status_counts[status] += 1
            elif status == ConversionStatus.CONTAIN_OTHER_FORMATS.name or \
                 status == ConversionStatus.NO_JPG_PNG_FOUND.name or \
                 status == ConversionStatus.NO_IMAGES_RECOGNIZED.name:
                other_image_type_archives_count += 1
                status_counts[status] += 1
            elif status == 'failed':
                status_counts['failed'] += 1
            elif status in status_counts: # For PROCESSED_SAVED_SPACE and PROCESSED_NO_SPACE_SAVED
                status_counts[status] += 1
            else:
                status_counts['unknown_status'] += 1


        console.print("\n[bold blue]--- Summary Statistics ---[/bold blue]")
        console.print(f"Total Archives Recorded: {len(data)}")

        if total_bytes_saved_overall >= 0:
            console.print(f"Total Space Saved (Overall): [green]{total_bytes_saved_overall / (1024**3):.3f} GB[/green] ({total_bytes_saved_overall / (1024**2):.2f} MB)")
        else:
            console.print(f"Total Space [red]Increased[/red] by (Overall): [red]{-total_bytes_saved_overall / (1024**3):.3f} GB[/red] ({-total_bytes_saved_overall / (1024**2):.2f} MB)")
        
        console.print(f"Total Space Saved (from successfully converted archives with positive saving): [green]{total_bytes_saved_from_converted / (1024**3):.3f} GB[/green] ({total_bytes_saved_from_converted / (1024**2):.2f} MB)")
        
        console.print("\n[bold blue]--- Archive Type Breakdown ---[/bold blue]")
        table = Table(title="Archive Processing Status Counts")
        table.add_column("Status", style="cyan")
        table.add_column("Count", style="magenta")

        for status_name, count in status_counts.items():
            table.add_row(status_name, str(count))
        
        console.print(table)

        if jxl_archives_count > 0 or other_image_type_archives_count > 0:
            ratio = jxl_archives_count / (other_image_type_archives_count + jxl_archives_count) if (other_image_type_archives_count + jxl_archives_count) > 0 else 0
            console.print(f"\nRatio of 'Already JXL' archives to 'Other Image Types': [bold]{jxl_archives_count}:{other_image_type_archives_count}[/bold] (Approx. {ratio:.2f} JXL per non-JXL image archive)")
        else:
            console.print("\nNo 'Already JXL' or 'Other Image Types' archives found to calculate ratio.")


    except sqlite3.Error as e:
        console.print(f"[red]Database query error: {e}[/red]")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    analyze_conversion_data()