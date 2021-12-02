# How to run Parquet benchmarks
- Enter in cpp folder
  - `cd arrow/cpp`
- Run cmake command to build the benchmark module
  ```sh
  $ mkdir -p build && cd build`
  $ cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DARROW_PARQUET=ON -DARROW_BUILD_TESTS=ON -DARROW_BUILD_BENCHMARKS=ON -DARROW_WITH_SNAPPY=ON ..
  $ cmake --build . --target parquet-arrow-reader-writer-benchmark
  ```
- Then export the environment variables and run the executable code
  ```shell
  $ export ARROW_PARQUET_BENCHMARK_COMPRESSED_FILE=[path-to-parquet-file]/compressed.parquet
  $ export ARROW_PARQUET_BENCHMARK_UNCOMPRESSED_FILE=[path-to-parquet-file]/uncompressed.parquet
  $ [path-to-arrow-directory]/arrow/cpp/cmake-build-debug/release/parquet-arrow-reader-writer-benchmark
  ```