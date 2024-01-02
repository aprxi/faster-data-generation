import sys
import random
import time
import argparse
from concurrent import futures

import polars as pl
from faker import Faker

VERSION = "0.0.1"

fake = Faker()


class ColumnTable:
    def __init__(self, count: int):
        self.ids = [
            random.randrange(1000, 9999999999999) for _ in range(count)
        ]
        self.first_names = [fake.first_name() for _ in range(count)]
        self.last_names = [fake.last_name() for _ in range(count)]
        self.emails = [fake.unique.ascii_email() for _ in range(count)]
        self.companies = [fake.company() for _ in range(count)]
        self.phone_numbers = [fake.phone_number() for _ in range(count)]


def generate_dataframe(no_rows: int, no_threads: int) -> pl.DataFrame:
    rows_per_thread = no_rows // no_threads

    with futures.ProcessPoolExecutor(max_workers=no_threads) as executor:
        # Submitting tasks
        tasks = [executor.submit(ColumnTable, rows_per_thread) for _ in range(no_threads)]

        # Collecting and combining results
        try:
            combined_data = {
                "ids": [],
                "first_names": [],
                "last_names": [],
                "emails": [],
                "companies": [],
                "phone_numbers": []
            }

            for future in futures.as_completed(tasks):
                result = future.result()
                combined_data["ids"].extend(result.ids)
                combined_data["first_names"].extend(result.first_names)
                combined_data["last_names"].extend(result.last_names)
                combined_data["emails"].extend(result.emails)
                combined_data["companies"].extend(result.companies)
                combined_data["phone_numbers"].extend(result.phone_numbers)

            return pl.DataFrame(combined_data)
        except Exception as exc:
            print(f"A task raised an exception: {exc}")
            return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Example script."
    )

    parser.add_argument(
        "-r",
        "--rows",
        type=int,
        default=10000,
        help="Number of rows"
    )
    parser.add_argument(
        "-t", 
        "--threads",
        type=int,
        default=1,
        help="Number of threads"
    )
    parser.add_argument(
        "-V",
        "--version",
        action="store_true",
        help="Print version"
    )

    # Parse the arguments
    args = parser.parse_args()

    if args.version:
        print(VERSION)
        return 0

    no_rows = args.rows
    no_threads = args.threads

    start_time = time.time()

    df = generate_dataframe(no_rows, no_threads)
    end_time = time.time()
    print(df)
    print(f"Time taken to generate {no_rows} people into a dataframe:")
    print(f"--- {round((end_time - start_time), 3)} seconds ---")

    start_time = time.time()
    df.write_parquet("people.parquet")
    end_time = time.time()
    print(f"Time taken to write to Parquet:")
    print(f"--- {round((end_time - start_time), 3)} seconds ---")
    return 0


if __name__ == "__main__":
    sys.exit(main())