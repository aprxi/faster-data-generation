import sys
import random
import time

from faker import Faker

NO_ROWS = 10000

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


def main() -> int:  
    start_time = time.time()
    table = ColumnTable(NO_ROWS)
    end_time = time.time()

    print("First 3 records:")
    for i in range(3):
        print(
            f"Record {i + 1}: {{ id: {table.ids[i]}, "
            f"first_name: \"{table.first_names[i]}\", "
            f"last_name: \"{table.last_names[i]}\", "
            f"email: \"{table.emails[i]}\", "
            f"company: \"{table.companies[i]}\", "
            f"phone_number: \"{table.phone_numbers[i]}\" }}"
        )

    print(f"Time taken to generate {NO_ROWS} people:")
    print(f"--- {round((end_time - start_time), 3)} seconds ---")
    return 0


if __name__ == "__main__":
    sys.exit(main())