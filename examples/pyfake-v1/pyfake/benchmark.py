import timeit

NO_ROWS = 10000
NO_EXECUTIONS = 10


def benchmark_column() -> None:
    setup_code = f"""
from pyfake.generate import ColumnTable
NO_ROWS = {NO_ROWS}
"""
    execution_time = timeit.timeit(
        "ColumnTable(NO_ROWS)",
        setup=setup_code,
        number=NO_EXECUTIONS,
    ) 
    print(f"Average time taken to generate {NO_ROWS} people:")
    print(f"--- {round((execution_time / NO_EXECUTIONS), 3)} seconds ---")


def benchmark_row() -> None:
    setup_code = f"""
from pyfake.generate_row import generate_person_list
NO_ROWS = {NO_ROWS}
"""
    execution_time = timeit.timeit(
        "generate_person_list(NO_ROWS)",
        setup=setup_code,
        number=NO_EXECUTIONS,
    ) 

    print(f"Average time taken to generate {NO_ROWS} people:")
    print(f"--- {round((execution_time / NO_EXECUTIONS), 3)} seconds ---")