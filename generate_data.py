from pathlib import Path
import pandas as pd
import random
from faker import Faker
import os

# Initialize Faker for realistic data generation
faker = Faker()

# Define column names
columns = [
    "driver",
    "client",
    "start_point",
    "end_point",
    "trip_start_time",
    "trip_end_time",
    "trip_cost",
    "driver_rating",
    "driver_feedback",
    "driver_text_feedback",
    "customer_rating",
    "customer_feedback",
]

ROWS_PER_CHUNK = 10_000
TARGET_SIZE_MB = 1024
ROW_SIZE_BYTES = 200
TOTAL_ROWS = (TARGET_SIZE_MB * 1024 * 1024) // ROW_SIZE_BYTES

feedback_categories = ["none", "car_condition", "route", "driver_attitude"]


def generate_row():
    start_time = faker.date_time_this_year()
    end_time = faker.date_time_between_dates(datetime_start=start_time)
    return {
        "driver": random.randint(1, 2000),
        "client": random.randint(1, 4000),
        "start_point": faker.address(),
        "end_point": faker.address(),
        "trip_start_time": start_time.isoformat(),
        "trip_end_time": end_time.isoformat(),
        "trip_cost": round(random.uniform(5, 200), 2),
        "driver_rating": random.randint(1, 5),
        "driver_feedback": random.choice(feedback_categories),
        "driver_text_feedback": faker.sentence(),
        "customer_rating": random.randint(1, 5),
        "customer_feedback": random.choice(feedback_categories),
    }


def generate_csv(file_path):
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(",".join(columns) + "\n")
        rows_written = 0
        while rows_written < TOTAL_ROWS:
            chunk = [generate_row() for _ in range(ROWS_PER_CHUNK)]
            chunk_df = pd.DataFrame(chunk)
            chunk_df.to_csv(f, index=False, header=False)
            rows_written += len(chunk)
            print(f"Rows written: {rows_written}/{TOTAL_ROWS}")


output_file = Path("data", "trips_data.csv")
output_file.parent.mkdir(parents=True, exist_ok=True)

generate_csv(output_file)
print(
    f"CSV file generated: {output_file}, size: {os.path.getsize(output_file) / (1024 * 1024):.2f} MB"
)
