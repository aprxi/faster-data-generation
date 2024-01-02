import sys
import random
import time

from faker import Faker
from typing import Any

NO_ROWS = 10000

fake = Faker()

def get_person() -> dict[str, Any]:
    person = {
        "id": random.randrange(1000, 9999999999999),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.unique.ascii_email(),
        "company": fake.company(),
        "phone": fake.phone_number()
    }
    return person


def generate_person_list(count: int) -> list[dict[str, Any]]:
    person_list = [get_person() for _ in range(count)]
    return person_list


def main() -> int:
    start_time = time.time()
    person_list = generate_person_list(NO_ROWS)
    end_time = time.time()

    print("First 3 records:", person_list[:3])
    print(f"Time taken to generate {NO_ROWS} people:")
    print(f"--- {round((end_time - start_time), 3)} seconds ---")


if __name__ == "__main__":
    sys.exit(main())