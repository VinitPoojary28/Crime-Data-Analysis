import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import numpy as np

# --- File path ---
file_path = 'D:/DUCKDB-WORKING/Dataset/Crime_Data_from_2020_to_Present.csv'

# --- Connect to DuckDB ---
con = duckdb.connect()

# --- Safe Date Parsing using CASE ---
date_expr = """
CASE
    WHEN "DATE OCC" LIKE '%/%' AND "DATE OCC" LIKE '%:% %M' THEN STRPTIME("DATE OCC", '%m/%d/%Y %I:%M:%S %p')
    WHEN "DATE OCC" LIKE '%-%' AND "DATE OCC" LIKE '%:%' THEN STRPTIME("DATE OCC", '%m-%d-%Y %H:%M')
    ELSE NULL
END
"""

# --- Month-wise Crime Trend Query ---
query = f"""
SELECT 
    STRFTIME({date_expr}, '%Y-%m') AS month,
    COUNT(*) AS total_crimes
FROM read_csv_auto('{file_path}')
WHERE "DATE OCC" IS NOT NULL
GROUP BY month
ORDER BY month;
"""

# --- Run Query and Convert to DataFrame ---
df = con.execute(query).df()

# --- Print top results ---
print("\n🔹 Month-wise Crime Trend:\n")
print(df.head(12))

# --- Save to CSV ---
df.to_csv("monthwise_crime_trend.csv", index=False)
print("\n✅ Crime trend saved to 'monthwise_crime_trend.csv'")

# --- Line Plot: Month-wise Crimes ---
plt.figure(figsize=(12, 6))
plt.plot(df['month'], df['total_crimes'], marker='o', linestyle='-', color='darkblue')
plt.xticks(rotation=45)
plt.title("Month-wise Crime Trend in LA")
plt.xlabel("Month")
plt.ylabel("Total Crimes")
plt.tight_layout()
plt.grid(True)
plt.show()

# --- Add Numeric Index for Regression ---
df['month_index'] = np.arange(len(df))
X = df[['month_index']]
y = df['total_crimes']

# --- Fit Linear Regression Model ---
model = LinearRegression()
model.fit(X, y)
trend = model.predict(X)

# --- Trend Line Plot ---
plt.figure(figsize=(14, 6))
plt.plot(df['month'], y, label='Monthly Crimes', marker='o')
plt.plot(df['month'], trend, label='Trend Line (Linear Regression)', color='red')
plt.xticks(rotation=45)
plt.title("Crime Trend with Linear Regression")
plt.xlabel("Month")
plt.ylabel("Total Crimes")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# --- Show Slope of Trend ---
print(f"\n📉 Slope of trend line: {model.coef_[0]:.2f}")
