import sqlite3 as sql
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

conn = sql.connect('eia_data.sqlite')
cur = conn.cursor()

# Pull Year range from Table
years = list()
cur.execute('SELECT year FROM clean_generation')
for row in cur :
      years.append(int(row[0]))
ymax = max(years)
ymin = min(years)

# User input and input validation
year = input(f'Enter a year between {ymin} and {ymax}: ')
try :
       year = int(year)
except: 
       print(
              f'Invalid input: {year}. '
              f'Please enter a valid number.'
       )
       quit()
if year < ymin or year > ymax :
       print(
              f'{year} is out of range. '
              f'Please enter a year within the stated range.'
       )
       quit()

# Aggregate net generation totals per fuel source
cur.execute('''
       SELECT fuel_code, SUM(generation) 
       FROM clean_generation
       WHERE year = ? AND fuel_code != "ALL"
       GROUP BY fuel_code
       ORDER BY SUM(generation) DESC
       ''', (year,))
rows = cur.fetchall()

# Create numpy arrays (required for matplotlib)
fuel_codes = []
generation = []

for row in rows :
       fuel_codes.append(row[0])
       generation.append(row[1])

fuel_codes = np.array(fuel_codes)
generation = np.array(generation)

# Plot the Top 10 fuel sources with matplotlib
plt.bar(fuel_codes[:10], generation[:10])
plt.title(f'Top 10 Net Electricity Generation for {year}')
plt.ylabel('Generation (MWh)')
plt.xlabel('Fuel Type')

# force plot to not use scientific notation and use comma separators
ax = plt.gca()
ax.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))

plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

# Program output
print(f'The top 10 net generating fuel sources of {year} in the U.S. were:')
for code, total in zip(fuel_codes[:10], generation[:10]) :
       print(f'{code} generated {round(total):,} MWh')

conn.close()