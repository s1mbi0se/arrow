// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "benchmark/benchmark.h"

#include <array>
#include <iostream>
#include <random>

#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/platform.h"

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/io/memory.h"
#include "arrow/io/file.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/logging.h"

using arrow::Array;
using arrow::ArrayVector;
using arrow::BooleanBuilder;
using arrow::FieldVector;
using arrow::NumericBuilder;

#define EXIT_NOT_OK(s)                                        \
  do {                                                        \
    ::arrow::Status _s = (s);                                 \
    if (ARROW_PREDICT_FALSE(!_s.ok())) {                      \
      std::cout << "Exiting: " << _s.ToString() << std::endl; \
      exit(EXIT_FAILURE);                                     \
    }                                                         \
  } while (0)

namespace parquet {

    using arrow::FileReader;
    using arrow::WriteTable;
    using schema::PrimitiveNode;

    namespace benchmark {

// This should result in multiple pages for most primitive types
        constexpr int64_t BENCHMARK_SIZE = 10 * 1024 * 1024;

        template<typename ParquetType>
        struct benchmark_traits {
        };

        template<>
        struct benchmark_traits<Int32Type> {
            using arrow_type = ::arrow::Int32Type;
        };

        template<>
        struct benchmark_traits<Int64Type> {
            using arrow_type = ::arrow::Int64Type;
        };

        template<>
        struct benchmark_traits<DoubleType> {
            using arrow_type = ::arrow::DoubleType;
        };

        template<>
        struct benchmark_traits<BooleanType> {
            using arrow_type = ::arrow::BooleanType;
        };

        template<typename ParquetType>
        using ArrowType = typename benchmark_traits<ParquetType>::arrow_type;

        template<typename ParquetType>
        std::shared_ptr<ColumnDescriptor> MakeSchema(Repetition::type repetition) {
            auto node = PrimitiveNode::Make("int64", repetition, ParquetType::type_num);
            return std::make_shared<ColumnDescriptor>(node, repetition != Repetition::REQUIRED,
                                                      repetition == Repetition::REPEATED);
        }

        template<bool nullable, typename ParquetType>
        void SetBytesProcessed(::benchmark::State &state, int64_t num_values = BENCHMARK_SIZE) {
            const int64_t items_processed = state.iterations() * num_values;
            const int64_t bytes_processed = items_processed * sizeof(typename ParquetType::c_type);

            state.SetItemsProcessed(bytes_processed);
            state.SetBytesProcessed(bytes_processed);
        }

        constexpr int64_t kAlternatingOrNa = -1;

        template<typename T>
        std::vector<T> RandomVector(int64_t true_percentage, int64_t vector_size,
                                    const std::array<T, 2> &sample_values, int seed = 500) {
            std::vector<T> values(vector_size, {});
            if (true_percentage == kAlternatingOrNa) {
                int n = {0};
                std::generate(values.begin(), values.end(), [&n] { return n++ % 2; });
            } else {
                std::default_random_engine rng(seed);
                double true_probability = static_cast<double>(true_percentage) / 100.0;
                std::bernoulli_distribution dist(true_probability);
                std::generate(values.begin(), values.end(), [&] { return sample_values[dist(rng)]; });
            }
            return values;
        }

        template<typename ParquetType>
        std::shared_ptr<::arrow::Table> TableFromVector(
                const std::vector<typename ParquetType::c_type> &vec, bool nullable,
                int64_t null_percentage = kAlternatingOrNa) {
            if (!nullable) {
                ARROW_CHECK_EQ(null_percentage, kAlternatingOrNa);
            }
            std::shared_ptr<::arrow::DataType> type = std::make_shared<ArrowType<ParquetType>>();
            NumericBuilder<ArrowType<ParquetType>> builder;
            if (nullable) {
                // Note true values select index 1 of sample_values
                auto valid_bytes = RandomVector<uint8_t>(/*true_percentage=*/null_percentage,
                                                                             vec.size(), /*sample_values=*/{1, 0});
                EXIT_NOT_OK(builder.AppendValues(vec.data(), vec.size(), valid_bytes.data()));
            } else {
                EXIT_NOT_OK(builder.AppendValues(vec.data(), vec.size(), nullptr));
            }
            std::shared_ptr<::arrow::Array> array;
            EXIT_NOT_OK(builder.Finish(&array));

            auto field = ::arrow::field("column", type, nullable);
            auto schema = ::arrow::schema({field});
            return ::arrow::Table::Make(schema, {array});
        }

        template<>
        std::shared_ptr<::arrow::Table> TableFromVector<BooleanType>(const std::vector<bool> &vec,
                                                                     bool nullable,
                                                                     int64_t null_percentage) {
            BooleanBuilder builder;
            if (nullable) {
                auto valid_bytes = RandomVector<bool>(/*true_percentage=*/null_percentage, vec.size(),
                                                                          {true, false});
                EXIT_NOT_OK(builder.AppendValues(vec, valid_bytes));
            } else {
                EXIT_NOT_OK(builder.AppendValues(vec));
            }
            std::shared_ptr<::arrow::Array> array;
            EXIT_NOT_OK(builder.Finish(&array));

            auto field = ::arrow::field("column", ::arrow::boolean(), nullable);
            auto schema = std::make_shared<::arrow::Schema>(
                    std::vector<std::shared_ptr<::arrow::Field>>({field}));
            return ::arrow::Table::Make(schema, {array});
        }

        template<bool nullable, typename ParquetType>
        static void BM_WriteColumn(::benchmark::State &state) {
            using T = typename ParquetType::c_type;
            std::vector<T> values(BENCHMARK_SIZE, static_cast<T>(128));
            std::shared_ptr<::arrow::Table> table = TableFromVector<ParquetType>(values, nullable);

            while (state.KeepRunning()) {
                auto output = CreateOutputStream();
                EXIT_NOT_OK(
                        WriteTable(*table, ::arrow::default_memory_pool(), output, BENCHMARK_SIZE));
            }
            SetBytesProcessed<nullable, ParquetType>(state);
        }

        template<typename T>
        struct Examples {
            static constexpr std::array<T, 2> values() { return {127, 128}; }
        };

        template<>
        struct Examples<bool> {
            static constexpr std::array<bool, 2> values() { return {false, true}; }
        };

        static void BM_Uncompressed_ReadOneColumnPerTimeInt(::benchmark::State &state) {
            ::arrow::MemoryPool *pool = ::arrow::default_memory_pool();

            // Open the parquet file
            std::shared_ptr<::arrow::io::RandomAccessFile> input;
            PARQUET_ASSIGN_OR_THROW(input, ::arrow::io::ReadableFile::Open("/home/anthonydremio/dremio_codes/multirowgroupparquetgenerator/uncompressed.parquet"));

            // Open Parquet file reader
            std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
            EXIT_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

            while (state.KeepRunning()) {

                std::shared_ptr<::arrow::RecordBatchReader> recordBatchReader;

                // Read just the first row group inside the table
                std::vector<int32_t> row_group_index;
                row_group_index.push_back(0);

                // Read just one column per time
                std::vector<int32_t> columns_to_read;
                columns_to_read.push_back(0);

                EXIT_NOT_OK(arrow_reader->GetRecordBatchReader(row_group_index, columns_to_read, &recordBatchReader));

                ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> result;
                do{
                    result = recordBatchReader->Next();
                }while(result.ValueOrDie());
            }
        }

        BENCHMARK(BM_Uncompressed_ReadOneColumnPerTimeInt)
            ->Unit(::benchmark::kMillisecond);

        static void BM_Uncompressed_ReadOneColumnPerTimeBigint(::benchmark::State &state) {
            ::arrow::MemoryPool *pool = ::arrow::default_memory_pool();

            // Open the parquet file
            std::shared_ptr<::arrow::io::RandomAccessFile> input;
            PARQUET_ASSIGN_OR_THROW(input, ::arrow::io::ReadableFile::Open("/home/anthonydremio/dremio_codes/multirowgroupparquetgenerator/uncompressed.parquet"));

            // Open Parquet file reader
            std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
            EXIT_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

            while (state.KeepRunning()) {

                std::shared_ptr<::arrow::RecordBatchReader> recordBatchReader;

                // Read just the first row group inside the table
                std::vector<int32_t> row_group_index;
                row_group_index.push_back(0);

                // Read just one column per time
                std::vector<int32_t> columns_to_read;
                columns_to_read.push_back(1);

                EXIT_NOT_OK(arrow_reader->GetRecordBatchReader(row_group_index, columns_to_read, &recordBatchReader));

                ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> result;
                do{
                    result = recordBatchReader->Next();
                }while(result.ValueOrDie());
            }
        }

        BENCHMARK(BM_Uncompressed_ReadOneColumnPerTimeBigint)
            ->Unit(::benchmark::kMillisecond);

        static void BM_Uncompressed_ReadOneColumnPerTimeVarchar(::benchmark::State &state) {
            ::arrow::MemoryPool *pool = ::arrow::default_memory_pool();

            // Open the parquet file
            std::shared_ptr<::arrow::io::RandomAccessFile> input;
            PARQUET_ASSIGN_OR_THROW(input, ::arrow::io::ReadableFile::Open("/home/anthonydremio/dremio_codes/multirowgroupparquetgenerator/uncompressed.parquet"));

            // Open Parquet file reader
            std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
            EXIT_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

            while (state.KeepRunning()) {

                std::shared_ptr<::arrow::RecordBatchReader> recordBatchReader;

                // Read just the first row group inside the table
                std::vector<int32_t> row_group_index;
                row_group_index.push_back(0);

                // Read just one column per time
                std::vector<int32_t> columns_to_read;
                columns_to_read.push_back(2);

                EXIT_NOT_OK(arrow_reader->GetRecordBatchReader(row_group_index, columns_to_read, &recordBatchReader));

                ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> result;
                do{
                    result = recordBatchReader->Next();
                }while(result.ValueOrDie());
            }
        }

        BENCHMARK(BM_Uncompressed_ReadOneColumnPerTimeVarchar)
            ->Unit(::benchmark::kMillisecond);

        static void BM_Uncompressed_ReadOneColumnPerTimeDecimal(::benchmark::State &state) {
            ::arrow::MemoryPool *pool = ::arrow::default_memory_pool();

            // Open the parquet file
            std::shared_ptr<::arrow::io::RandomAccessFile> input;
            PARQUET_ASSIGN_OR_THROW(input, ::arrow::io::ReadableFile::Open("/home/anthonydremio/dremio_codes/multirowgroupparquetgenerator/uncompressed.parquet"));

            // Open Parquet file reader
            std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
            EXIT_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

            while (state.KeepRunning()) {

                std::shared_ptr<::arrow::RecordBatchReader> recordBatchReader;

                // Read just the first row group inside the table
                std::vector<int32_t> row_group_index;
                row_group_index.push_back(0);

                // Read just one column per time
                std::vector<int32_t> columns_to_read;
                columns_to_read.push_back(3);

                EXIT_NOT_OK(arrow_reader->GetRecordBatchReader(row_group_index, columns_to_read, &recordBatchReader));

                ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> result;
                do{
                    result = recordBatchReader->Next();
                }while(result.ValueOrDie());
            }
        }

        BENCHMARK(BM_Uncompressed_ReadOneColumnPerTimeDecimal)
            ->Unit(::benchmark::kMillisecond);

        static void BM_Uncompressed_ReadOneColumnPerTimeStruct(::benchmark::State &state) {
            ::arrow::MemoryPool *pool = ::arrow::default_memory_pool();

            // Open the parquet file
            std::shared_ptr<::arrow::io::RandomAccessFile> input;
            PARQUET_ASSIGN_OR_THROW(input, ::arrow::io::ReadableFile::Open("/home/anthonydremio/dremio_codes/multirowgroupparquetgenerator/uncompressed.parquet"));

            // Open Parquet file reader
            std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
            EXIT_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

            while (state.KeepRunning()) {

                std::shared_ptr<::arrow::RecordBatchReader> recordBatchReader;

                // Read just the first row group inside the table
                std::vector<int32_t> row_group_index;
                row_group_index.push_back(0);

                // Read just one column per time
                std::vector<int32_t> columns_to_read;
                columns_to_read.push_back(4);

                EXIT_NOT_OK(arrow_reader->GetRecordBatchReader(row_group_index, columns_to_read, &recordBatchReader));

                ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> result;
                do{
                    result = recordBatchReader->Next();
                }while(result.ValueOrDie());
            }
        }

        BENCHMARK(BM_Uncompressed_ReadOneColumnPerTimeStruct)
            ->Unit(::benchmark::kMillisecond);


        static void BM_Uncompressed_ReadOneColumnPerTimeArrayStruct(::benchmark::State &state) {
            ::arrow::MemoryPool *pool = ::arrow::default_memory_pool();

            // Open the parquet file
            std::shared_ptr<::arrow::io::RandomAccessFile> input;
            PARQUET_ASSIGN_OR_THROW(input, ::arrow::io::ReadableFile::Open("/home/anthonydremio/dremio_codes/multirowgroupparquetgenerator/uncompressed.parquet"));

            // Open Parquet file reader
            std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
            EXIT_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

            while (state.KeepRunning()) {

                std::shared_ptr<::arrow::RecordBatchReader> recordBatchReader;

                // Read just the first row group inside the table
                std::vector<int32_t> row_group_index;
                row_group_index.push_back(0);

                // Read just one column per time
                std::vector<int32_t> columns_to_read;
                columns_to_read.push_back(5);

                EXIT_NOT_OK(arrow_reader->GetRecordBatchReader(row_group_index, columns_to_read, &recordBatchReader));

                ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> result;
                do{
                    result = recordBatchReader->Next();
                }while(result.ValueOrDie());
            }
        }

        BENCHMARK(BM_Uncompressed_ReadOneColumnPerTimeArrayStruct)
            ->Unit(::benchmark::kMillisecond);

        static void BM_Compressed_ReadOneColumnPerTimeInt(::benchmark::State &state) {
            ::arrow::MemoryPool *pool = ::arrow::default_memory_pool();

            // Open the parquet file
            std::shared_ptr<::arrow::io::RandomAccessFile> input;
            PARQUET_ASSIGN_OR_THROW(input, ::arrow::io::ReadableFile::Open("/home/anthonydremio/dremio_codes/multirowgroupparquetgenerator/compressed.parquet"));

            // Open Parquet file reader
            std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
            EXIT_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

            while (state.KeepRunning()) {

                std::shared_ptr<::arrow::RecordBatchReader> recordBatchReader;

                // Read just the first row group inside the table
                std::vector<int32_t> row_group_index;
                row_group_index.push_back(0);

                // Read just one column per time
                std::vector<int32_t> columns_to_read;
                columns_to_read.push_back(0);

                EXIT_NOT_OK(arrow_reader->GetRecordBatchReader(row_group_index, columns_to_read, &recordBatchReader));

                ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> result;
                do{
                    result = recordBatchReader->Next();
                }while(result.ValueOrDie());
            }
        }

        BENCHMARK(BM_Compressed_ReadOneColumnPerTimeInt)
            ->Unit(::benchmark::kMillisecond);

        static void BM_Compressed_ReadOneColumnPerTimeBigint(::benchmark::State &state) {
            ::arrow::MemoryPool *pool = ::arrow::default_memory_pool();

            // Open the parquet file
            std::shared_ptr<::arrow::io::RandomAccessFile> input;
            PARQUET_ASSIGN_OR_THROW(input, ::arrow::io::ReadableFile::Open("/home/anthonydremio/dremio_codes/multirowgroupparquetgenerator/compressed.parquet"));

            // Open Parquet file reader
            std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
            EXIT_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

            while (state.KeepRunning()) {

                std::shared_ptr<::arrow::RecordBatchReader> recordBatchReader;

                // Read just the first row group inside the table
                std::vector<int32_t> row_group_index;
                row_group_index.push_back(0);

                // Read just one column per time
                std::vector<int32_t> columns_to_read;
                columns_to_read.push_back(1);

                EXIT_NOT_OK(arrow_reader->GetRecordBatchReader(row_group_index, columns_to_read, &recordBatchReader));

                ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> result;
                do{
                    result = recordBatchReader->Next();
                }while(result.ValueOrDie());
            }
        }

        BENCHMARK(BM_Compressed_ReadOneColumnPerTimeBigint)
            ->Unit(::benchmark::kMillisecond);

        static void BM_Compressed_ReadOneColumnPerTimeVarchar(::benchmark::State &state) {
            ::arrow::MemoryPool *pool = ::arrow::default_memory_pool();

            // Open the parquet file
            std::shared_ptr<::arrow::io::RandomAccessFile> input;
            PARQUET_ASSIGN_OR_THROW(input, ::arrow::io::ReadableFile::Open("/home/anthonydremio/dremio_codes/multirowgroupparquetgenerator/compressed.parquet"));

            // Open Parquet file reader
            std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
            EXIT_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

            while (state.KeepRunning()) {

                std::shared_ptr<::arrow::RecordBatchReader> recordBatchReader;

                // Read just the first row group inside the table
                std::vector<int32_t> row_group_index;
                row_group_index.push_back(0);

                // Read just one column per time
                std::vector<int32_t> columns_to_read;
                columns_to_read.push_back(2);

                EXIT_NOT_OK(arrow_reader->GetRecordBatchReader(row_group_index, columns_to_read, &recordBatchReader));

                ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> result;
                do{
                    result = recordBatchReader->Next();
                }while(result.ValueOrDie());
            }
        }

        BENCHMARK(BM_Compressed_ReadOneColumnPerTimeVarchar)
            ->Unit(::benchmark::kMillisecond);

        static void BM_Compressed_ReadOneColumnPerTimeDecimal(::benchmark::State &state) {
            ::arrow::MemoryPool *pool = ::arrow::default_memory_pool();

            // Open the parquet file
            std::shared_ptr<::arrow::io::RandomAccessFile> input;
            PARQUET_ASSIGN_OR_THROW(input, ::arrow::io::ReadableFile::Open("/home/anthonydremio/dremio_codes/multirowgroupparquetgenerator/compressed.parquet"));

            // Open Parquet file reader
            std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
            EXIT_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

            while (state.KeepRunning()) {

                std::shared_ptr<::arrow::RecordBatchReader> recordBatchReader;

                // Read just the first row group inside the table
                std::vector<int32_t> row_group_index;
                row_group_index.push_back(0);

                // Read just one column per time
                std::vector<int32_t> columns_to_read;
                columns_to_read.push_back(3);

                EXIT_NOT_OK(arrow_reader->GetRecordBatchReader(row_group_index, columns_to_read, &recordBatchReader));

                ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> result;
                do{
                    result = recordBatchReader->Next();
                }while(result.ValueOrDie());
            }
        }

        BENCHMARK(BM_Compressed_ReadOneColumnPerTimeDecimal)
            ->Unit(::benchmark::kMillisecond);

        static void BM_Compressed_ReadOneColumnPerTimeStruct(::benchmark::State &state) {
            ::arrow::MemoryPool *pool = ::arrow::default_memory_pool();

            // Open the parquet file
            std::shared_ptr<::arrow::io::RandomAccessFile> input;
            PARQUET_ASSIGN_OR_THROW(input, ::arrow::io::ReadableFile::Open("/home/anthonydremio/dremio_codes/multirowgroupparquetgenerator/compressed.parquet"));

            // Open Parquet file reader
            std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
            EXIT_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

            while (state.KeepRunning()) {

                std::shared_ptr<::arrow::RecordBatchReader> recordBatchReader;

                // Read just the first row group inside the table
                std::vector<int32_t> row_group_index;
                row_group_index.push_back(0);

                // Read just one column per time
                std::vector<int32_t> columns_to_read;
                columns_to_read.push_back(4);

                EXIT_NOT_OK(arrow_reader->GetRecordBatchReader(row_group_index, columns_to_read, &recordBatchReader));

                ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> result;
                do{
                    result = recordBatchReader->Next();
                }while(result.ValueOrDie());
            }
        }

        BENCHMARK(BM_Compressed_ReadOneColumnPerTimeStruct)
            ->Unit(::benchmark::kMillisecond);

        static void BM_Compressed_ReadOneColumnPerTimeArrayStruct(::benchmark::State &state) {
            ::arrow::MemoryPool *pool = ::arrow::default_memory_pool();

            // Open the parquet file
            std::shared_ptr<::arrow::io::RandomAccessFile> input;
            PARQUET_ASSIGN_OR_THROW(input, ::arrow::io::ReadableFile::Open("/home/anthonydremio/dremio_codes/multirowgroupparquetgenerator/compressed.parquet"));

            // Open Parquet file reader
            std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
            EXIT_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

            while (state.KeepRunning()) {

                std::shared_ptr<::arrow::RecordBatchReader> recordBatchReader;

                // Read just the first row group inside the table
                std::vector<int32_t> row_group_index;
                row_group_index.push_back(0);

                // Read just one column per time
                std::vector<int32_t> columns_to_read;
                columns_to_read.push_back(5);

                EXIT_NOT_OK(arrow_reader->GetRecordBatchReader(row_group_index, columns_to_read, &recordBatchReader));

                ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> result;
                do{
                    result = recordBatchReader->Next();
                }while(result.ValueOrDie());
            }
        }

        BENCHMARK(BM_Compressed_ReadOneColumnPerTimeArrayStruct)
            ->Unit(::benchmark::kMillisecond);

    }  // namespace benchmark

}  // namespace parquet
