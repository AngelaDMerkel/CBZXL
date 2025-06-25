import sqlite3
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import argparse
import base64
from io import BytesIO
from rich.console import Console
from rich.table import Table

# --- Constants ---
DB_FILE = "converted_archives.db"
FAILED_DB_FILE = "failed_archives.db"

# --- Initial Setup ---
console = Console()
plt.style.use('seaborn-v0_8-whitegrid')

# --- Data Loading ---
def load_dataframes():
    """Loads data from both databases into pandas DataFrames."""
    processed_df, failed_df = None, None
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

# --- HTML Generation ---
def fig_to_base64(fig):
    """Converts a Matplotlib figure to a Base64 encoded string."""
    buf = BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    return base64.b64encode(buf.getvalue()).decode('utf-8')

def generate_html_report(stats_html_list, plot_html_parts):
    """Generates the full HTML report string from parts."""
    stats_section = "".join(stats_html_list)
    plots_section = "".join(plot_html_parts)

    style = """
    <style>
        body { font-family: sans-serif; margin: 2em; background-color: #f0f0f0; color: #333; }
        h1, h2 { color: #1e1e1e; border-bottom: 2px solid #ccc; padding-bottom: 5px; }
        .container { max-width: 1200px; margin: auto; background-color: white; padding: 1em 2em; box-shadow: 0 0 15px rgba(0,0,0,0.1); border-radius: 8px;}
        .plot-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(500px, 1fr)); gap: 2em; align-items: start; }
        .plot { text-align: center; margin-bottom: 2em; padding: 1em; background-color: #f9f9f9; border-radius: 5px;}
        .plot img { max-width: 100%; height: auto; }
    </style>
    """
    body = f"""
    <div class="container">
        <h1>CBZ Conversion Report</h1>
        <h2>Statistics</h2>
        {stats_section}
        <h2>Visualizations</h2>
        <div class="plot-grid">
            {plots_section}
        </div>
    </div>
    """
    return f"<!DOCTYPE html><html><head><title>CBZ Conversion Report</title>{style}</head><body>{body}</body></html>"


# --- Data Analysis and Plotting ---
def get_statistics_tables(df):
    """Returns a list of Rich tables with detailed statistics."""
    if df.empty: return []

    tables = []
    total_saved_mb = df['bytes_saved'].sum() / (1024 * 1024)
    df_for_mean = df[df['percent_saved'] >= 0.5]
    mean_percent_saved = df_for_mean['percent_saved'].mean() if not df_for_mean.empty else 0.0
    
    stats_table = Table(title="Conversion Summary", title_style="bold magenta")
    stats_table.add_column("Metric", style="cyan"); stats_table.add_column("Value", style="bold green")
    stats_table.add_row("Total Space Saved", f"{total_saved_mb:.2f} MB")
    stats_table.add_row("Average Saving Percentage (>=0.5%)", f"{mean_percent_saved:.2f}%")
    stats_table.add_row("Median Saving Percentage (all files)", f"{df['percent_saved'].median():.2f}%")
    stats_table.add_row("Best Saving Percentage", f"{df['percent_saved'].max():.2f}%")
    stats_table.add_row("Worst Saving Percentage", f"{df['percent_saved'].min():.2f}%")
    tables.append(stats_table)

    best_table = Table(title="Top 5 Best Conversions (by % Saved)", title_style="bold magenta")
    best_table.add_column("File Path", style="green", no_wrap=True); best_table.add_column("% Saved", style="bold green")
    for _, row in df.nlargest(5, 'percent_saved').iterrows():
        best_table.add_row(row['path'], f"{row['percent_saved']:.2f}%")
    tables.append(best_table)
    
    worst_table = Table(title="Top 5 Worst Conversions (by % Saved)", title_style="bold magenta")
    worst_table.add_column("File Path", style="red", no_wrap=True); worst_table.add_column("% Saved", style="bold green")
    for _, row in df.nsmallest(5, 'percent_saved').iterrows():
        worst_table.add_row(row['path'], f"{row['percent_saved']:.2f}%")
    tables.append(worst_table)
    
    return tables

def plot_savings_distribution(df, to_html=False):
    if df.empty or 'percent_saved' not in df.columns: return None
    df_to_plot = df[df['bytes_saved'] != 0]
    if df_to_plot.empty: 
        console.print("\n[yellow]No archives with size changes found to plot distribution.[/yellow]")
        return None

    fig, ax = plt.subplots(figsize=(10, 6))
    df_to_plot['percent_saved'].plot(kind='hist', bins=30, color='skyblue', ec='black', ax=ax)
    ax.set_title('Distribution of Saving Percentages (for converted archives)', fontsize=16)
    ax.set_xlabel('Saving Percentage (%)', fontsize=12); ax.set_ylabel('Number of Archives', fontsize=12)
    mean_val = df_to_plot['percent_saved'].mean()
    ax.axvline(mean_val, color='red', linestyle='dashed', linewidth=2, label=f"Mean: {mean_val:.2f}%")
    ax.legend()
    plt.tight_layout()
    if to_html: return fig
    console.print("\n[bold]Displaying plot 1: Distribution of Saving Percentages...[/bold]"); plt.show()
    plt.close(fig)

def plot_size_vs_savings(df, to_html=False):
    if df.empty or 'percent_saved' not in df.columns: return None
    df['original_size_mb'] = df['original_size'] / (1024 * 1024)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(df['original_size_mb'], df['percent_saved'], alpha=0.5)
    ax.set_title('Original File Size vs. Saving Percentage', fontsize=16)
    ax.set_xlabel('Original File Size (MB)', fontsize=12); ax.set_ylabel('Saving Percentage (%)', fontsize=12)
    ax.set_xscale('log')
    plt.tight_layout()
    if to_html: return fig
    console.print("[bold]Displaying plot 2: Original Size vs. Saving Percentage...[/bold]"); plt.show()
    plt.close(fig)

def plot_summary_pie(processed_count, failed_count, to_html=False):
    if processed_count == 0 and failed_count == 0: return None
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.pie([processed_count, failed_count], explode=((0.1, 0) if processed_count > 0 else (0, 0)),
           labels=['Processed', 'Failed'], colors=['lightgreen', 'lightcoral'],
           autopct='%1.1f%%', shadow=True, startangle=140)
    ax.axis('equal')
    ax.set_title('Overall Summary: Processed vs. Failed Archives', fontsize=16)
    if to_html: return fig
    console.print("[bold]Displaying plot 3: Overall Summary Pie Chart...[/bold]"); plt.show()
    plt.close(fig)

def plot_cumulative_savings(df, to_html=False):
    if df.empty or 'bytes_saved' not in df.columns or 'converted_at' not in df.columns: return None
    df['converted_at'] = pd.to_datetime(df['converted_at']); df = df.sort_values(by='converted_at')
    df['cumulative_saved_mb'] = df['bytes_saved'].cumsum() / (1024 * 1024)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['converted_at'], df['cumulative_saved_mb'], marker='.', linestyle='-', markersize=4)
    ax.set_title('Cumulative Space Saved Over Time', fontsize=16)
    ax.set_xlabel('Date of Conversion', fontsize=12); ax.set_ylabel('Cumulative Space Saved (MB)', fontsize=12)
    ax.grid(True, which="both", ls="--")
    plt.tight_layout()
    if to_html: return fig
    console.print("[bold]Displaying plot 4: Cumulative Space Saved...[/bold]"); plt.show()
    plt.close(fig)

def plot_size_distribution(df, to_html=False):
    """Plots a box plot comparing original and final file sizes for converted archives."""
    if df.empty or 'original_size' not in df.columns or 'final_size' not in df.columns: return None
    
    # UPDATED: Filter out archives with no change in size for a more meaningful distribution.
    df_to_plot = df[df['bytes_saved'] != 0].copy()

    if df_to_plot.empty:
        console.print("\n[yellow]No archives with size changes found to plot size distribution.[/yellow]")
        return None

    # Convert sizes to MB for plotting
    df_to_plot['original_size_mb'] = df_to_plot['original_size'] / (1024 * 1024)
    df_to_plot['final_size_mb'] = df_to_plot['final_size'] / (1024 * 1024)

    fig, ax = plt.subplots(figsize=(8, 7))
    sns.boxplot(data=df_to_plot[['original_size_mb', 'final_size_mb']], palette="Set2", ax=ax)
    ax.set_title('Distribution of Original vs. Final Archive Sizes (for converted archives)', fontsize=16)
    ax.set_ylabel('File Size (MB)', fontsize=12)
    plt.tight_layout()
    if to_html: return fig
    console.print("[bold]Displaying plot 5: Original vs. Final Size Distribution...[/bold]"); plt.show()
    plt.close(fig)

def plot_savings_by_type(df, to_html=False):
    if df.empty or 'dominant_type' not in df.columns or 'percent_saved' not in df.columns:
        console.print("\n[yellow]Cannot generate savings by type: 'dominant_type' column not found.[/yellow]")
        return None
    df_filtered = df[(df['dominant_type'].isin(['JPG', 'PNG', 'Mixed'])) & (df['bytes_saved'] != 0)]
    if df_filtered.empty:
        console.print("\n[yellow]No data available to compare savings by image type.[/yellow]")
        return None
    fig, ax = plt.subplots(figsize=(10, 7))
    sns.boxplot(x='dominant_type', y='percent_saved', data=df_filtered, palette="pastel", ax=ax)
    sns.stripplot(x='dominant_type', y='percent_saved', data=df_filtered, color=".25", size=3, ax=ax)
    ax.set_title('Saving Percentage by Dominant Image Type', fontsize=16)
    ax.set_xlabel('Dominant Original Image Type', fontsize=12); ax.set_ylabel('Saving Percentage (%)', fontsize=12)
    plt.tight_layout()
    if to_html: return fig
    console.print("[bold]Displaying plot 6: Saving Percentage by Image Type...[/bold]"); plt.show()
    plt.close(fig)

def main():
    """Main function to run the analysis."""
    parser = argparse.ArgumentParser(description="Visualize CBZ conversion data.")
    parser.add_argument("--html-report", type=str, help="Generate an HTML report instead of displaying plots. Provide filename.")
    args = parser.parse_args()

    console.print("\n[bold green]--- Conversion Data Visualizer ---[/bold green]")
    processed_df, failed_df = load_dataframes()
    processed_count = len(processed_df) if processed_df is not None else 0
    failed_count = len(failed_df) if failed_df is not None else 0

    if processed_count == 0 and failed_count == 0:
        console.print("[bold red]No data found in either database. Exiting.[/bold red]")
        return
        
    # --- HTML Report Generation ---
    if args.html_report:
        console.print(f"Generating HTML report at [cyan]{args.html_report}[/cyan]...")
        
        stats_html_parts = []
        if processed_count > 0:
            stats_tables = get_statistics_tables(processed_df)
            for table in stats_tables:
                capture_console = Console(record=True, width=120)
                capture_console.print(table)
                stats_html_parts.append(capture_console.export_html(inline_styles=True))
        
        plot_html_parts = []
        plot_functions = [
            plot_savings_distribution, plot_size_vs_savings, 
            lambda df, to_html: plot_summary_pie(processed_count, failed_count, to_html),
            plot_cumulative_savings, plot_size_distribution, plot_savings_by_type
        ]
        
        for i, plot_func in enumerate(plot_functions):
            df_copy = processed_df.copy() if processed_df is not None else pd.DataFrame()
            fig = plot_func(df_copy, to_html=True)
            if fig:
                b64_img = fig_to_base64(fig)
                plot_html_parts.append(f'<div class="plot"><img src="data:image/png;base64,{b64_img}" alt="Plot {i+1}"></div>')
                plt.close(fig)

        html_content = generate_html_report(stats_html_parts, plot_html_parts)
        try:
            with open(args.html_report, 'w', encoding='utf-8') as f:
                f.write(html_content)
            console.print(f"[bold green]Successfully created report: {args.html_report}[/bold green]")
        except IOError as e:
            console.print(f"[red]Error writing HTML file: {e}[/red]")

    # --- Interactive Mode ---
    else:
        if processed_count > 0:
            tables = get_statistics_tables(processed_df)
            for table in tables:
                console.print(table)
            
            plot_savings_distribution(processed_df.copy())
            plot_size_vs_savings(processed_df.copy())
            plot_summary_pie(processed_count, failed_count)
            plot_cumulative_savings(processed_df.copy())
            plot_size_distribution(processed_df.copy())
            plot_savings_by_type(processed_df.copy())
        else:
            console.print("\n[yellow]No processed archives found to generate statistics or plots.[/yellow]")
            if failed_count > 0:
                plot_summary_pie(processed_count, failed_count)

    console.print("\n[bold green]--- Analysis Complete ---[/bold green]")

if __name__ == "__main__":
    main()
