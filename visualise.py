import sqlite3
import os
import pandas as pd
import matplotlib.pyplot as plt
from rich.console import Console
from rich.table import Table

# --- Constants ---
# These should match the constants in your main processing script
DB_FILE = "converted_archives.db"
FAILED_DB_FILE = "failed_archives.db"

# --- Initial Setup ---
console = Console()
# Set a style for the plots
plt.style.use('seaborn-v0_8-whitegrid')


def load_dataframes():
    """Loads data from both databases into pandas DataFrames."""
    processed_df = None
    failed_df = None

    if os.path.exists(DB_FILE):
        try:
            conn = sqlite3.connect(DB_FILE)
            processed_df = pd.read_sql_query("SELECT * FROM converted_archives", conn)
            console.print(f"[green]Successfully loaded {len(processed_df)} records from '{DB_FILE}'[/green]")
            conn.close()
        except sqlite3.Error as e:
            console.print(f"[red]Error reading database '{DB_FILE}': {e}[/red]")
    else:
        console.print(f"[yellow]Warning: Database file not found at '{DB_FILE}'[/yellow]")

    if os.path.exists(FAILED_DB_FILE):
        try:
            conn = sqlite3.connect(FAILED_DB_FILE)
            failed_df = pd.read_sql_query("SELECT * FROM converted_archives", conn)
            console.print(f"[green]Successfully loaded {len(failed_df)} records from '{FAILED_DB_FILE}'[/green]")
            conn.close()
        except sqlite3.Error as e:
            console.print(f"[red]Error reading database '{FAILED_DB_FILE}': {e}[/red]")
    else:
        console.print(f"[yellow]Warning: Database file not found at '{FAILED_DB_FILE}'[/yellow]")

    return processed_df, failed_df


def display_statistics(df):
    """Calculates and displays detailed statistics using a Rich table."""
    console.print("\n[bold cyan]--- Detailed Statistics ---[/bold cyan]")

    if df.empty:
        console.print("[yellow]No data available for statistics.[/yellow]")
        return

    # Calculate statistics
    total_saved_mb = df['bytes_saved'].sum() / (1024 * 1024)
    mean_percent_saved = df['percent_saved'].mean()
    median_percent_saved = df['percent_saved'].median()
    max_percent_saved = df['percent_saved'].max()
    min_percent_saved = df['percent_saved'].min()
    
    # Get top 5 best and worst conversions by percentage
    top_5_best = df.nlargest(5, 'percent_saved')
    top_5_worst = df.nsmallest(5, 'percent_saved')

    # Create and populate the main stats table
    stats_table = Table(title="Conversion Summary")
    stats_table.add_column("Metric", style="cyan")
    stats_table.add_column("Value", style="magenta")
    
    stats_table.add_row("Total Space Saved", f"{total_saved_mb:.2f} MB")
    stats_table.add_row("Average Saving Percentage", f"{mean_percent_saved:.2f}%")
    stats_table.add_row("Median Saving Percentage", f"{median_percent_saved:.2f}%")
    stats_table.add_row("Best Saving Percentage", f"{max_percent_saved:.2f}%")
    stats_table.add_row("Worst Saving Percentage", f"{min_percent_saved:.2f}%")
    
    console.print(stats_table)

    # Create and populate the "Top 5 Best" table
    best_table = Table(title="Top 5 Best Conversions (by % Saved)")
    best_table.add_column("File Path", style="green", no_wrap=True)
    best_table.add_column("% Saved", style="magenta")
    for _, row in top_5_best.iterrows():
        best_table.add_row(row['path'], f"{row['percent_saved']:.2f}%")
    
    console.print(best_table)
    
    # Create and populate the "Top 5 Worst" table
    worst_table = Table(title="Top 5 Worst Conversions (by % Saved)")
    worst_table.add_column("File Path", style="red", no_wrap=True)
    worst_table.add_column("% Saved", style="magenta")
    for _, row in top_5_worst.iterrows():
        worst_table.add_row(row['path'], f"{row['percent_saved']:.2f}%")
        
    console.print(worst_table)


def plot_savings_distribution(df):
    """Plots a histogram of the saving percentages."""
    if df.empty or 'percent_saved' not in df.columns:
        return
        
    plt.figure(figsize=(10, 6))
    df['percent_saved'].plot(kind='hist', bins=30, color='skyblue', ec='black')
    plt.title('Distribution of Saving Percentages', fontsize=16)
    plt.xlabel('Saving Percentage (%)', fontsize=12)
    plt.ylabel('Number of Archives', fontsize=12)
    plt.axvline(df['percent_saved'].mean(), color='red', linestyle='dashed', linewidth=2, label=f"Mean: {df['percent_saved'].mean():.2f}%")
    plt.legend()
    plt.tight_layout()
    console.print("\n[bold]Displaying plot 1: Distribution of Saving Percentages...[/bold]")
    plt.show()


def plot_size_vs_savings(df):
    """Plots original file size vs. saving percentage."""
    if df.empty or 'percent_saved' not in df.columns:
        return

    # Create a new column for original size in MB for better plotting
    df['original_size_mb'] = df['original_size'] / (1024 * 1024)

    plt.figure(figsize=(10, 6))
    plt.scatter(df['original_size_mb'], df['percent_saved'], alpha=0.5)
    plt.title('Original File Size vs. Saving Percentage', fontsize=16)
    plt.xlabel('Original File Size (MB)', fontsize=12)
    plt.ylabel('Saving Percentage (%)', fontsize=12)
    plt.xscale('log') # Use a log scale for size as it can vary greatly
    plt.tight_layout()
    console.print("[bold]Displaying plot 2: Original Size vs. Saving Percentage...[/bold]")
    plt.show()


def plot_summary_pie(processed_count, failed_count):
    """Plots a pie chart of processed vs. failed archives."""
    if processed_count == 0 and failed_count == 0:
        return
        
    labels = 'Processed', 'Failed'
    sizes = [processed_count, failed_count]
    colors = ['lightgreen', 'lightcoral']
    explode = (0.1, 0) if processed_count > 0 else (0, 0)

    plt.figure(figsize=(8, 8))
    plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
            shadow=True, startangle=140)
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.title('Overall Summary: Processed vs. Failed Archives', fontsize=16)
    console.print("[bold]Displaying plot 3: Overall Summary Pie Chart...[/bold]")
    plt.show()


def main():
    """Main function to run the analysis."""
    console.print("\n[bold green]--- Conversion Data Visualizer ---[/bold green]")
    processed_df, failed_df = load_dataframes()

    processed_count = len(processed_df) if processed_df is not None else 0
    failed_count = len(failed_df) if failed_df is not None else 0

    if processed_count > 0:
        display_statistics(processed_df)
        
        # Generate Plots
        plot_savings_distribution(processed_df.copy())
        plot_size_vs_savings(processed_df.copy())
        plot_summary_pie(processed_count, failed_count)
    else:
        console.print("\n[yellow]No processed archives found to generate statistics or plots.[/yellow]")
        if failed_count > 0:
            plot_summary_pie(processed_count, failed_count)

    console.print("\n[bold green]--- Analysis Complete ---[/bold green]")


if __name__ == "__main__":
    main()
