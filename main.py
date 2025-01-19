import psycopg2
from psycopg2 import sql
from jobspy import scrape_jobs
import os

# Define connection parameters for PostgreSQL
PG_HOST = os.getenv("PG_HOST")
PG_DBNAME = os.getenv("PG_DBNAME")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT", 5432)            # default PostgreSQL port

# Define job categories to scrape
job_categories = [
    "Software Engineering",
    "Engineering and Development", 
    "Data Analysis",
    "Accounting and Finance",
    "Machine Learning and AI",
    "Consulting",
    "Marketing",
    "Management and Executive",
    "Product Management",
    "Arts and Entertainment",
    "Legal and Compliance",
    "Education and Training",
    "Business Analyst",
    "Creatives and Design",
    "Customer Service and Support",
    "Human Resources",
    "Public Sector and Government",
    "Sales"
]

# Define location list
locations = [
    "New Delhi",
    "Mumbai",
    "Bengaluru",
    "Chennai",
    "Kolkata",
    "Hyderabad",
    "Pune",
    "Ahmedabad",
    "Jaipur",
    "Lucknow"
]

country_name = "India"

# Load proxy list from file or define manually
proxies = [
    "44.195.247.145:80", "184.169.154.119:80", "3.21.101.158:3128",
    "203.77.215.45:10000", "3.90.100.12:80", "47.251.43.115:33333",
    "102.223.186.246:8888", "203.144.144.146:8080", "103.152.112.120:80",
    "23.247.136.245:80", "23.247.136.254:80", "103.152.112.157:80",
    "13.208.56.180:80", "35.72.118.126:80", "43.202.154.212:80",
    "35.76.62.196:80", "35.79.120.242:3128", "18.228.149.161:80",
    "3.139.242.184:80", "43.200.77.128:3128", "43.201.121.81:80",
    "54.233.119.172:3128", "18.228.198.164:80", "52.67.10.183:80",
    "204.236.176.61:3128", "54.152.3.36:80", "54.248.238.110:80",
    "162.19.107.209:3128", "63.35.64.177:3128", "134.209.23.180:8888",
    "204.236.137.68:80", "114.35.140.157:8080", "41.59.90.171:80",
    "141.11.103.136:8080", "44.219.175.186:80", "158.255.77.166:80",
    "23.247.137.142:80", "20.205.61.143:80", "178.128.113.118:23128",
    "133.18.234.13:80"
]


# Test proxies to find working ones
# def test_proxy(proxy):
#     """Test if a proxy is working."""
#     test_url = "https://httpbin.org/ip"
#     try:
#         response = requests.get(test_url, proxies={"http": proxy, "https": proxy}, timeout=5)
#         if response.status_code == 200:
#             return True
#     except Exception:
#         return False

# # Filter working proxies
# working_proxies = [proxy for proxy in proxies if test_proxy(proxy)]
# print(f"Working proxies: {working_proxies}")

# 1) Connect to PostgreSQL database
conn = psycopg2.connect(
    host=PG_HOST,
    dbname=PG_DBNAME,
    user=PG_USER,
    password=PG_PASSWORD,
    port=PG_PORT,
    sslmode='require'
)
conn.autocommit = True  # Enable auto commit for DDL statements
cursor = conn.cursor()

print("Connected to PostgreSQL database!")

def create_category_table(category):
    """Create a PostgreSQL table for the given category if it doesn't exist."""
    table_name = category.lower().replace(" ", "_")
    
    # Construct a SQL statement for table creation using psycopg2.sql for safety
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            id SERIAL PRIMARY KEY,
            site TEXT NOT NULL,
            title TEXT,
            company TEXT,
            city TEXT,
            state TEXT,
            job_type TEXT,
            interval TEXT,
            min_amount REAL,
            max_amount REAL,
            job_url TEXT UNIQUE,
            description TEXT
        )
    """).format(table=sql.Identifier(table_name))
    
    cursor.execute(create_table_query)
    return table_name

# 2) Loop over each job category and city, then scrape and save data
for category in job_categories:
    table_name = create_category_table(category)
    
    for city in locations:
        print(f"Scraping category='{category}', city='{city}'...")
        
        try:
            jobs_df = scrape_jobs(
                site_name=["indeed", "linkedin", "glassdoor", "zip_recruiter", "google"],
                search_term=category,
                location=city,
                country_indeed=country_name,
                results_wanted=20,
                verbose=1,
                linkedin_fetch_description=True,
                proxies=proxies
            )
        except Exception as e:
            print(f"Error scraping category='{category}', city='{city}': {e}")
            continue
        
        print(f"   Found {len(jobs_df)} jobs for category='{category}', city='{city}'.")
        
        # Standardize column names and ensure expected columns
        jobs_df = jobs_df.rename(columns=str.upper)
        expected_columns = [
            "SITE", "TITLE", "COMPANY", "CITY", "STATE", "JOB_TYPE",
            "INTERVAL", "MIN_AMOUNT", "MAX_AMOUNT", "JOB_URL", "DESCRIPTION"
        ]
        jobs_df = jobs_df.reindex(columns=expected_columns, fill_value="")

        # 3) Insert rows into PostgreSQL table
        for _, row in jobs_df.iterrows():
            try:
                insert_query = sql.SQL("""
                    INSERT INTO {table} (
                        site, title, company, city, state, 
                        job_type, interval, min_amount, 
                        max_amount, job_url, description
                    ) VALUES (
                        %s, %s, %s, %s, %s, 
                        %s, %s, %s, 
                        %s, %s, %s
                    )
                    ON CONFLICT (job_url) DO NOTHING
                """).format(table=sql.Identifier(table_name))
                
                cursor.execute(insert_query, (
                    row["SITE"],
                    row["TITLE"],
                    row["COMPANY"],
                    row["CITY"],
                    row["STATE"],
                    row["JOB_TYPE"],
                    row["INTERVAL"],
                    row["MIN_AMOUNT"] if row["MIN_AMOUNT"] != "" else None,
                    row["MAX_AMOUNT"] if row["MAX_AMOUNT"] != "" else None,
                    row["JOB_URL"],
                    row["DESCRIPTION"]
                ))
            except Exception as e:
                print(f"Error inserting job into table {table_name}: {e}")

# 4) Close connections
cursor.close()
conn.close()
print("All jobs have been scraped and saved to the PostgreSQL database!")
