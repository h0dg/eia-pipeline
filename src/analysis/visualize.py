from src.db import Database
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


# -----------------------------
# Data Selection / Validation
# -----------------------------

def desired_year(clean_db, year_input: str = None) -> int:
    """
    Validate and return a desired year for plotting.

    :param clean_db: Database object for clean data
    :param year_input: Optional; string or int input. If None, prompt user
    :return: int, validated year
    :raises ValueError: if year is invalid or out of range
    """
    ymax, ymin = clean_db.pull_year_range()

    if year_input is None:
        year_input = input(f"Enter a year between {ymin} and {ymax}: ")

    try:
        year = int(year_input)
    except Exception:
        raise ValueError(f"Invalid input: {year_input}. Must be a number.")

    if year < ymin or year > ymax:
        raise ValueError(f"{year} is out of range. Must be between {ymin} and {ymax}.")

    return year


# -----------------------------
# Data Aggregation
# -----------------------------

def create_arrays(clean_db, year: int):
    """
    Aggregate net generation totals per fuel source for a given year.

    :param clean_db: Database object for clean data
    :param year: int, year to aggregate
    :return: tuple of (fuel_codes, generation, top10_list)
    """
    fuel_codes, generation = [], []

    for row in clean_db.aggregate_generation(year):
        fuel_codes.append(row[0])
        generation.append(row[1])

    fuel_codes = np.array(fuel_codes)
    generation = np.array(generation)

    top10 = [(code, round(total)) for code, total in zip(fuel_codes[:10], generation[:10])]

    print(f"\nTop 10 net generating fuel sources of {year} in the U.S.:")
    for code, total in top10:
        print(f"{code} generated {total:,} MWh")

    return fuel_codes, generation, top10


# -----------------------------
# Visualization
# -----------------------------

def plot_top10(fuel_codes, generation, year: int):
    """
    Plot top 10 electricity generation fuel sources as a bar chart.

    :param fuel_codes: array-like, fuel codes
    :param generation: array-like, generation values (same order as fuel_codes)
    :param year: int, year of data
    """
    plt.figure(figsize=(10, 6))
    plt.bar(fuel_codes[:10], generation[:10], color='skyblue')
    plt.title(f"Top 10 Net Electricity Generation for {year}", fontsize=14)
    plt.ylabel("Generation (MWh)", fontsize=12)
    plt.xlabel("Fuel Type", fontsize=12)

    # Format y-axis with commas
    ax = plt.gca()
    ax.yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:,.0f}"))

    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()


# -----------------------------
# Main Runner
# -----------------------------

def main():
    """
    Standalone runner for the visualization module.
    Prompts user for a year and plots top 10 fuel sources.
    """
    clean_db = Database("clean")
    try:
        year = desired_year(clean_db)
        fuel_codes, generation, top10 = create_arrays(clean_db, year)
        plot_top10(fuel_codes, generation, year)
    finally:
        clean_db.close()


if __name__ == "__main__":
    main()
