# Generating data 100x faster with Rust

Example scripts to demonstrate the ability of Rust to accelerate data generation. Performance improvement can vary, initial measurements currently show 100-150x gain for a typical dataset/ configuration in v1.

v2 version is still a work in progress, although most functionality is working it may have some rough edges. Performance with Rayon threading is still something I am exploring, when scaling up it does not saturate all the cores perfectly. If you know why feel free to send me a PR. 

PRs on any other performance improvement are welcome, as the goal of this project is to get the fastest data generation possible. While we are currently at 100x -- have a sense we can push this much higher over time.

## Versions
### v1 (Python / Rust)
* generate dataset

### v2 (Python / Rust)
* ability to pass parameters via CLI
* dynamic schema loading (Rust-only)
* enable threading (multi-core)
* convert to dataframe
* export to Parquet

## Run examples

### Python

```
# enter directory
cd examples/pyfake-v1

# v1 requires faker
pip install faker
# v2 requires faker and polars
pip install faker polars

# run
python pyfake/generate.py

# benchmark row (average over 10 runs)
python -c 'import pyfake; pyfake.benchmark_row()'

# benchmark column (average over 10 runs)
python -c 'import pyfake; pyfake.benchmark_column()'
```

### Python with Poetry
```
# enter directory
cd examples/pyfake-v1

# install dependencies
poetry update

# run script
poetry run pyfake
```

### Rust
```
# enter directory
cd examples/rsfake-v1

cargo build --release
target/release/rsfake
```

### Docker
For convenience a Dockerfile is included with both Python and Rust dependencies pre-installed.

```
# build
docker build -t fakeroo .

# run interactive shell
docker run -ti  --rm fakeroo bash

# run Python example
cd /examples/pyfake-v1
python pyfake/generate.py

# run Rust example
cd /examples/rsfake-v1
bin/rsfake
```