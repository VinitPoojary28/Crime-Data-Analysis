import duckdb
import pandas as pd
import matplotlib.pyplot as plt

# ------------------ File Setup ------------------
file_path = "D:\DUCKDB-WORKING\Dataset\Crime_Data_from_2020_to_Present.csv"

con = duckdb.connect()

# --- Intelligent Date Parsing using CASE ---
date_expr = """
CASE
    WHEN "DATE OCC" LIKE '%/%' AND "DATE OCC" LIKE '%:% %M' THEN STRPTIME("DATE OCC", '%m/%d/%Y %I:%M:%S %p')
    WHEN "DATE OCC" LIKE '%-%' AND "DATE OCC" LIKE '%:%' THEN STRPTIME("DATE OCC", '%m-%d-%Y %H:%M')
    ELSE NULL
END
"""

# 1. Month-wise Trend
query_monthly_trend = f"""
SELECT 
    STRFTIME({date_expr}, '%Y-%m') AS month,
    COUNT(*) AS total_crimes
FROM read_csv_auto('{file_path}')
WHERE "DATE OCC" IS NOT NULL
GROUP BY month
ORDER BY month;
"""
df_monthly = con.execute(query_monthly_trend).df()

# 2. Seasonality
query_seasonality = f"""
SELECT 
    STRFTIME({date_expr}, '%m') AS month_number,
    STRFTIME({date_expr}, '%m') || '-' || STRFTIME({date_expr}, '%Y') AS month_label,
    COUNT(*) AS total_crimes
FROM read_csv_auto('{file_path}')
WHERE "DATE OCC" IS NOT NULL
GROUP BY month_number, month_label
ORDER BY month_label;
"""
df_seasonal = con.execute(query_seasonality).df()

# 3. Crime Type Per Month
query_crime_type_month = f"""
SELECT 
    STRFTIME({date_expr}, '%Y-%m') AS month,
    "Crm Cd Desc" AS crime_type,
    COUNT(*) AS count
FROM read_csv_auto('{file_path}')
WHERE "DATE OCC" IS NOT NULL AND "Crm Cd Desc" IS NOT NULL
GROUP BY month, crime_type
ORDER BY month;
"""
df_crime_type = con.execute(query_crime_type_month).df()

# 4. Clearance Rate
query_clearance_rate = f"""
SELECT 
    STRFTIME({date_expr}, '%Y-%m') AS month,
    COUNT(*) FILTER (WHERE "Status" = 'CC') * 1.0 / COUNT(*) AS clearance_rate
FROM read_csv_auto('{file_path}')
WHERE "DATE OCC" IS NOT NULL AND "Status" IS NOT NULL
GROUP BY month
ORDER BY month;
"""
df_clearance = con.execute(query_clearance_rate).df()

# --- Plotting ---
plt.figure(figsize=(14, 6))
plt.plot(df_monthly['month'], df_monthly['total_crimes'], marker='o', color='darkgreen')
plt.xticks(rotation=45)
plt.title("📊 Month-wise Crime Trend in LA")
plt.xlabel("Month")
plt.ylabel("Total Crimes")
plt.grid(True)
plt.tight_layout()
plt.show()