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

#include <gmock/gmock-matchers.h>

#include <random>
#include <unordered_set>

#include "arrow/api.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/kernels/row_encoder.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/pcg_random.h"
#include "arrow/util/thread_pool.h"

using testing::UnorderedElementsAreArray;

namespace arrow {
namespace compute {

BatchesWithSchema GenerateBatchesFromString(
    const std::shared_ptr<Schema>& schema,
    const std::vector<util::string_view>& json_strings, int multiplicity = 1) {
  BatchesWithSchema out_batches{{}, schema};

  std::vector<ValueDescr> descrs;
  for (auto&& field : schema->fields()) {
    descrs.emplace_back(field->type());
  }

  for (auto&& s : json_strings) {
    out_batches.batches.push_back(ExecBatchFromJSON(descrs, s));
  }

  size_t batch_count = out_batches.batches.size();
  for (int repeat = 1; repeat < multiplicity; ++repeat) {
    for (size_t i = 0; i < batch_count; ++i) {
      out_batches.batches.push_back(out_batches.batches[i]);
    }
  }

  return out_batches;
}

void CheckRunOutput(JoinType type, const BatchesWithSchema& l_batches,
                    const BatchesWithSchema& r_batches,
                    const std::vector<FieldRef>& left_keys,
                    const std::vector<FieldRef>& right_keys,
                    const BatchesWithSchema& exp_batches, bool parallel = false) {
  auto exec_ctx = arrow::internal::make_unique<ExecContext>(
      default_memory_pool(), parallel ? arrow::internal::GetCpuThreadPool() : nullptr);

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make(exec_ctx.get()));

  HashJoinNodeOptions join_options{type, left_keys, right_keys};
  Declaration join{"hashjoin", join_options};

  // add left source
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{l_batches.schema, l_batches.gen(parallel,
                                                                  /*slow=*/false)}});
  // add right source
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r_batches.schema, r_batches.gen(parallel,
                                                                  /*slow=*/false)}});
  AsyncGenerator<util::optional<ExecBatch>> sink_gen;

  ASSERT_OK(Declaration::Sequence({join, {"sink", SinkNodeOptions{&sink_gen}}})
                .AddToPlan(plan.get()));

  ASSERT_FINISHES_OK_AND_ASSIGN(auto res, StartAndCollect(plan.get(), sink_gen));

  ASSERT_OK_AND_ASSIGN(auto exp_table,
                       TableFromExecBatches(exp_batches.schema, exp_batches.batches));

  ASSERT_OK_AND_ASSIGN(auto out_table, TableFromExecBatches(exp_batches.schema, res));

  if (exp_table->num_rows() == 0) {
    ASSERT_EQ(exp_table->num_rows(), out_table->num_rows());
  } else {
    std::vector<SortKey> sort_keys;
    for (auto&& f : exp_batches.schema->fields()) {
      sort_keys.emplace_back(f->name());
    }
    ASSERT_OK_AND_ASSIGN(auto exp_table_sort_ids,
                         SortIndices(exp_table, SortOptions(sort_keys)));
    ASSERT_OK_AND_ASSIGN(auto exp_table_sorted, Take(exp_table, exp_table_sort_ids));
    ASSERT_OK_AND_ASSIGN(auto out_table_sort_ids,
                         SortIndices(out_table, SortOptions(sort_keys)));
    ASSERT_OK_AND_ASSIGN(auto out_table_sorted, Take(out_table, out_table_sort_ids));

    AssertTablesEqual(*exp_table_sorted.table(), *out_table_sorted.table(),
                      /*same_chunk_layout=*/false, /*flatten=*/true);
  }
}

void RunNonEmptyTest(JoinType type, bool parallel) {
  auto l_schema = schema({field("l_i32", int32()), field("l_str", utf8())});
  auto r_schema = schema({field("r_str", utf8()), field("r_i32", int32())});
  BatchesWithSchema l_batches, r_batches, exp_batches;

  int multiplicity = parallel ? 100 : 1;

  l_batches = GenerateBatchesFromString(
      l_schema,
      {R"([[0,"d"], [1,"b"]])", R"([[2,"d"], [3,"a"], [4,"a"]])",
       R"([[5,"b"], [6,"c"], [7,"e"], [8,"e"]])"},
      multiplicity);

  r_batches = GenerateBatchesFromString(
      r_schema,
      {R"([["f", 0], ["b", 1], ["b", 2]])", R"([["c", 3], ["g", 4]])", R"([["e", 5]])"},
      multiplicity);

  switch (type) {
    case JoinType::LEFT_SEMI:
      exp_batches = GenerateBatchesFromString(
          l_schema, {R"([[1,"b"]])", R"([])", R"([[5,"b"], [6,"c"], [7,"e"], [8,"e"]])"},
          multiplicity);
      break;
    case JoinType::RIGHT_SEMI:
      exp_batches = GenerateBatchesFromString(
          r_schema, {R"([["b", 1], ["b", 2]])", R"([["c", 3]])", R"([["e", 5]])"},
          multiplicity);
      break;
    case JoinType::LEFT_ANTI:
      exp_batches = GenerateBatchesFromString(
          l_schema, {R"([[0,"d"]])", R"([[2,"d"], [3,"a"], [4,"a"]])", R"([])"},
          multiplicity);
      break;
    case JoinType::RIGHT_ANTI:
      exp_batches = GenerateBatchesFromString(
          r_schema, {R"([["f", 0]])", R"([["g", 4]])", R"([])"}, multiplicity);
      break;
    case JoinType::INNER:
    case JoinType::LEFT_OUTER:
    case JoinType::RIGHT_OUTER:
    case JoinType::FULL_OUTER:
    default:
      FAIL() << "join type not implemented!";
  }

  CheckRunOutput(type, l_batches, r_batches,
                 /*left_keys=*/{{"l_str"}}, /*right_keys=*/{{"r_str"}}, exp_batches,
                 parallel);
}

void RunEmptyTest(JoinType type, bool parallel) {
  auto l_schema = schema({field("l_i32", int32()), field("l_str", utf8())});
  auto r_schema = schema({field("r_str", utf8()), field("r_i32", int32())});

  int multiplicity = parallel ? 100 : 1;

  BatchesWithSchema l_empty, r_empty, l_n_empty, r_n_empty;

  l_empty = GenerateBatchesFromString(l_schema, {R"([])"}, multiplicity);
  r_empty = GenerateBatchesFromString(r_schema, {R"([])"}, multiplicity);

  l_n_empty =
      GenerateBatchesFromString(l_schema, {R"([[0,"d"], [1,"b"]])"}, multiplicity);
  r_n_empty = GenerateBatchesFromString(r_schema, {R"([["f", 0], ["b", 1], ["b", 2]])"},
                                        multiplicity);

  std::vector<FieldRef> l_keys{{"l_str"}};
  std::vector<FieldRef> r_keys{{"r_str"}};

  switch (type) {
    case JoinType::LEFT_SEMI:
      // both empty
      CheckRunOutput(type, l_empty, r_empty, l_keys, r_keys, l_empty, parallel);
      // right empty
      CheckRunOutput(type, l_n_empty, r_empty, l_keys, r_keys, l_empty, parallel);
      // left empty
      CheckRunOutput(type, l_empty, r_n_empty, l_keys, r_keys, l_empty, parallel);
      break;
    case JoinType::RIGHT_SEMI:
      // both empty
      CheckRunOutput(type, l_empty, r_empty, l_keys, r_keys, r_empty, parallel);
      // right empty
      CheckRunOutput(type, l_n_empty, r_empty, l_keys, r_keys, r_empty, parallel);
      // left empty
      CheckRunOutput(type, l_empty, r_n_empty, l_keys, r_keys, r_empty, parallel);
      break;
    case JoinType::LEFT_ANTI:
      // both empty
      CheckRunOutput(type, l_empty, r_empty, l_keys, r_keys, l_empty, parallel);
      // right empty
      CheckRunOutput(type, l_n_empty, r_empty, l_keys, r_keys, l_n_empty, parallel);
      // left empty
      CheckRunOutput(type, l_empty, r_n_empty, l_keys, r_keys, l_empty, parallel);
      break;
    case JoinType::RIGHT_ANTI:
      // both empty
      CheckRunOutput(type, l_empty, r_empty, l_keys, r_keys, r_empty, parallel);
      // right empty
      CheckRunOutput(type, l_n_empty, r_empty, l_keys, r_keys, r_empty, parallel);
      // left empty
      CheckRunOutput(type, l_empty, r_n_empty, l_keys, r_keys, r_n_empty, parallel);
      break;
    case JoinType::INNER:
    case JoinType::LEFT_OUTER:
    case JoinType::RIGHT_OUTER:
    case JoinType::FULL_OUTER:
    default:
      FAIL() << "join type not implemented!";
  }
}

class HashJoinTest : public testing::TestWithParam<std::tuple<JoinType, bool>> {};

INSTANTIATE_TEST_SUITE_P(
    HashJoinTest, HashJoinTest,
    ::testing::Combine(::testing::Values(JoinType::LEFT_SEMI, JoinType::RIGHT_SEMI,
                                         JoinType::LEFT_ANTI, JoinType::RIGHT_ANTI),
                       ::testing::Values(false, true)));

TEST_P(HashJoinTest, TestSemiJoins) {
  RunNonEmptyTest(std::get<0>(GetParam()), std::get<1>(GetParam()));
}

TEST_P(HashJoinTest, TestSemiJoinsEmpty) {
  RunEmptyTest(std::get<0>(GetParam()), std::get<1>(GetParam()));
}

class Random64Bit {
 public:
  explicit Random64Bit(random::SeedType seed) : rng_(seed) {}
  uint64_t next() { return dist_(rng_); }
  template <typename T>
  inline T from_range(const T& min_val, const T& max_val) {
    return static_cast<T>(min_val + (next() % (max_val - min_val + 1)));
  }

 private:
  random::pcg32_fast rng_;
  std::uniform_int_distribution<uint64_t> dist_;
};

struct RandomDataTypeConstraints {
  int64_t data_type_enabled_mask;
  // Null related
  double min_null_probability;
  double max_null_probability;
  // Binary related
  int min_binary_length;
  int max_binary_length;
  // String related
  int min_string_length;
  int max_string_length;

  void Default() {
    data_type_enabled_mask = kInt1 | kInt2 | kInt4 | kInt8 | kBool | kBinary | kString;
    min_null_probability = 0.0;
    max_null_probability = 0.2;
    min_binary_length = 1;
    max_binary_length = 40;
    min_string_length = 0;
    max_string_length = 40;
  }

  void OnlyInt(int int_size, bool allow_nulls) {
    Default();
    data_type_enabled_mask =
        int_size == 8 ? kInt8 : int_size == 4 ? kInt4 : int_size == 2 ? kInt2 : kInt1;
    if (!allow_nulls) {
      max_null_probability = 0.0;
    }
  }

  void OnlyString(bool allow_nulls) {
    Default();
    data_type_enabled_mask = kString;
    if (!allow_nulls) {
      max_null_probability = 0.0;
    }
  }

  // Data type mask constants
  static constexpr int64_t kInt1 = 1;
  static constexpr int64_t kInt2 = 2;
  static constexpr int64_t kInt4 = 4;
  static constexpr int64_t kInt8 = 8;
  static constexpr int64_t kBool = 16;
  static constexpr int64_t kBinary = 32;
  static constexpr int64_t kString = 64;
};

struct RandomDataType {
  double null_probability;
  bool is_fixed_length;
  int fixed_length;
  int min_string_length;
  int max_string_length;

  static RandomDataType Random(Random64Bit& rng,
                               const RandomDataTypeConstraints& constraints) {
    RandomDataType result;
    if ((constraints.data_type_enabled_mask & constraints.kString) != 0) {
      if (constraints.data_type_enabled_mask != constraints.kString) {
        // Both string and fixed length types enabled
        // 50% chance of string
        result.is_fixed_length = ((rng.next() % 2) == 0);
      } else {
        result.is_fixed_length = false;
      }
    } else {
      result.is_fixed_length = true;
    }
    if (constraints.max_null_probability > 0.0) {
      // 25% chance of no nulls
      // Uniform distribution of null probability from min to max
      result.null_probability = ((rng.next() % 4) == 0)
                                    ? 0.0
                                    : static_cast<double>(rng.next() % 1025) / 1024.0 *
                                              (constraints.max_null_probability -
                                               constraints.min_null_probability) +
                                          constraints.min_null_probability;
    } else {
      result.null_probability = 0.0;
    }
    // Pick data type for fixed length
    if (result.is_fixed_length) {
      int log_type;
      for (;;) {
        log_type = rng.next() % 6;
        if (constraints.data_type_enabled_mask & (1ULL << log_type)) {
          break;
        }
      }
      if ((1ULL << log_type) == constraints.kBinary) {
        for (;;) {
          result.fixed_length = rng.from_range(constraints.min_binary_length,
                                               constraints.max_binary_length);
          if (result.fixed_length != 1 && result.fixed_length != 2 &&
              result.fixed_length != 4 && result.fixed_length != 8) {
            break;
          }
        }
      } else {
        result.fixed_length =
            ((1ULL << log_type) == constraints.kBool) ? 0 : (1ULL << log_type);
      }
    } else {
      // Pick parameters for string
      result.min_string_length =
          rng.from_range(constraints.min_string_length, constraints.max_string_length);
      result.max_string_length =
          rng.from_range(constraints.min_string_length, constraints.max_string_length);
      if (result.min_string_length > result.max_string_length) {
        std::swap(result.min_string_length, result.max_string_length);
      }
    }
    return result;
  }
};

struct RandomDataTypeVector {
  std::vector<RandomDataType> data_types;

  void AddRandom(Random64Bit& rng, const RandomDataTypeConstraints& constraints) {
    data_types.push_back(RandomDataType::Random(rng, constraints));
  }

  void Print() {
    for (size_t i = 0; i < data_types.size(); ++i) {
      if (!data_types[i].is_fixed_length) {
        std::cout << "str[" << data_types[i].min_string_length << ".."
                  << data_types[i].max_string_length << "]";
        SCOPED_TRACE("str[" + std::to_string(data_types[i].min_string_length) + ".." +
                     std::to_string(data_types[i].max_string_length) + "]");
      } else {
        std::cout << "int[" << data_types[i].fixed_length << "]";
        SCOPED_TRACE("int[" + std::to_string(data_types[i].fixed_length) + "]");
      }
    }
    std::cout << std::endl;
  }
};

std::vector<std::shared_ptr<Array>> GenRandomRecords(
    Random64Bit& rng, const std::vector<RandomDataType>& data_types, int num_rows) {
  std::vector<std::shared_ptr<Array>> result;
  random::RandomArrayGenerator rag(static_cast<random::SeedType>(rng.next()));
  for (size_t i = 0; i < data_types.size(); ++i) {
    if (data_types[i].is_fixed_length) {
      switch (data_types[i].fixed_length) {
        case 0:
          result.push_back(rag.Boolean(num_rows, 0.5, data_types[i].null_probability));
          break;
        case 1:
          result.push_back(rag.UInt8(num_rows, std::numeric_limits<uint8_t>::min(),
                                     std::numeric_limits<uint8_t>::max(),
                                     data_types[i].null_probability));
          break;
        case 2:
          result.push_back(rag.UInt16(num_rows, std::numeric_limits<uint16_t>::min(),
                                      std::numeric_limits<uint16_t>::max(),
                                      data_types[i].null_probability));
          break;
        case 4:
          result.push_back(rag.UInt32(num_rows, std::numeric_limits<uint32_t>::min(),
                                      std::numeric_limits<uint32_t>::max(),
                                      data_types[i].null_probability));
          break;
        case 8:
          result.push_back(rag.UInt64(num_rows, std::numeric_limits<uint64_t>::min(),
                                      std::numeric_limits<uint64_t>::max(),
                                      data_types[i].null_probability));
          break;
        default:
          result.push_back(rag.FixedSizeBinary(num_rows, data_types[i].fixed_length,
                                               data_types[i].null_probability));
          break;
      }
    } else {
      result.push_back(rag.String(num_rows, data_types[i].min_string_length,
                                  data_types[i].max_string_length,
                                  data_types[i].null_probability));
    }
  }
  return result;
}

// Index < 0 means appending null values to all columns.
//
void TakeUsingVector(ExecContext* ctx, const std::vector<std::shared_ptr<Array>>& input,
                     const std::vector<int32_t> indices,
                     std::vector<std::shared_ptr<Array>>* result) {
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Buffer> buf,
      AllocateBuffer(indices.size() * sizeof(int32_t), ctx->memory_pool()));
  int32_t* buf_indices = reinterpret_cast<int32_t*>(buf->mutable_data());
  bool has_null_rows = false;
  for (size_t i = 0; i < indices.size(); ++i) {
    if (indices[i] < 0) {
      buf_indices[i] = 0;
      has_null_rows = true;
    } else {
      buf_indices[i] = indices[i];
    }
  }
  std::shared_ptr<Array> indices_array = MakeArray(ArrayData::Make(
      int32(), indices.size(), {nullptr, std::move(buf)}, /*null_count=*/0));

  result->resize(input.size());
  for (size_t i = 0; i < result->size(); ++i) {
    ASSERT_OK_AND_ASSIGN(Datum new_array, Take(input[i], indices_array));
    (*result)[i] = new_array.make_array();
  }
  if (has_null_rows) {
    for (size_t i = 0; i < result->size(); ++i) {
      if ((*result)[i]->data()->buffers[0] == NULLPTR) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> null_buf,
                             AllocateBitmap(indices.size(), ctx->memory_pool()));
        uint8_t* non_nulls = null_buf->mutable_data();
        memset(non_nulls, 0xFF, BitUtil::BytesForBits(indices.size()));
        if ((*result)[i]->data()->buffers.size() == 2) {
          (*result)[i] = MakeArray(
              ArrayData::Make((*result)[i]->type(), indices.size(),
                              {std::move(null_buf), (*result)[i]->data()->buffers[1]}));
        } else {
          (*result)[i] = MakeArray(
              ArrayData::Make((*result)[i]->type(), indices.size(),
                              {std::move(null_buf), (*result)[i]->data()->buffers[1],
                               (*result)[i]->data()->buffers[2]}));
        }
      }
      (*result)[i]->data()->SetNullCount(kUnknownNullCount);
    }
    for (size_t i = 0; i < indices.size(); ++i) {
      if (indices[i] < 0) {
        for (size_t col = 0; col < result->size(); ++col) {
          uint8_t* non_nulls = (*result)[col]->data()->buffers[0]->mutable_data();
          BitUtil::ClearBit(non_nulls, i);
        }
      }
    }
  }
}

// Generate random arrays given list of data type descriptions and null probabilities.
// Make sure that all generated records are unique.
// The actual number of generated records may be lower than desired because duplicates
// will be removed without replacement.
//
std::vector<std::shared_ptr<Array>> GenRandomUniqueRecords(
    Random64Bit& rng, const RandomDataTypeVector& data_types, int num_desired,
    int* num_actual) {
  std::vector<std::shared_ptr<Array>> result =
      GenRandomRecords(rng, data_types.data_types, num_desired);

  ExecContext* ctx = default_exec_context();
  std::vector<ValueDescr> val_descrs;
  for (size_t i = 0; i < result.size(); ++i) {
    val_descrs.push_back(ValueDescr(result[i]->type(), ValueDescr::ARRAY));
  }
  internal::RowEncoder encoder;
  encoder.Init(val_descrs, ctx);
  ExecBatch batch({}, num_desired);
  batch.values.resize(result.size());
  for (size_t i = 0; i < result.size(); ++i) {
    batch.values[i] = result[i];
  }
  Status status = encoder.EncodeAndAppend(batch);
  ARROW_DCHECK(status.ok());

  std::unordered_map<std::string, int> uniques;
  std::vector<int32_t> ids;
  for (int i = 0; i < num_desired; ++i) {
    if (uniques.find(encoder.encoded_row(i)) == uniques.end()) {
      uniques.insert(std::make_pair(encoder.encoded_row(i), i));
      ids.push_back(i);
    }
  }
  *num_actual = static_cast<int>(uniques.size());

  std::vector<std::shared_ptr<Array>> output;
  TakeUsingVector(ctx, result, ids, &output);
  return output;
}

std::vector<bool> NullInKey(const std::vector<JoinKeyCmp>& cmp,
                            const std::vector<std::shared_ptr<Array>>& key) {
  ARROW_DCHECK(cmp.size() <= key.size());
  ARROW_DCHECK(key.size() > 0);
  std::vector<bool> result;
  result.resize(key[0]->length());
  for (size_t i = 0; i < result.size(); ++i) {
    result[i] = false;
  }
  for (size_t i = 0; i < cmp.size(); ++i) {
    if (cmp[i] != JoinKeyCmp::EQ) {
      continue;
    }
    if (key[i]->data()->buffers[0] == NULLPTR) {
      continue;
    }
    const uint8_t* nulls = key[i]->data()->buffers[0]->data();
    if (!nulls) {
      continue;
    }
    for (size_t j = 0; j < result.size(); ++j) {
      if (!BitUtil::GetBit(nulls, j)) {
        result[j] = true;
      }
    }
  }
  return result;
}

void GenRandomJoinTables(ExecContext* ctx, Random64Bit& rng, int num_rows_l,
                         int num_rows_r, int num_keys_common, int num_keys_left,
                         int num_keys_right, const RandomDataTypeVector& key_types,
                         const RandomDataTypeVector& payload_left_types,
                         const RandomDataTypeVector& payload_right_types,
                         std::vector<int32_t>* key_id_l, std::vector<int32_t>* key_id_r,
                         std::vector<std::shared_ptr<Array>>* left,
                         std::vector<std::shared_ptr<Array>>* right) {
  // Generate random keys dictionary
  //
  int num_keys_desired = num_keys_left + num_keys_right - num_keys_common;
  int num_keys_actual = 0;
  std::vector<std::shared_ptr<Array>> keys =
      GenRandomUniqueRecords(rng, key_types, num_keys_desired, &num_keys_actual);

  // There will be three dictionary id ranges:
  // - common keys [0..num_keys_common-1]
  // - keys on right that are not on left [num_keys_common..num_keys_right-1]
  // - keys on left that are not on right [num_keys_right..num_keys_actual-1]
  //
  num_keys_common = static_cast<int>(static_cast<int64_t>(num_keys_common) *
                                     num_keys_actual / num_keys_desired);
  num_keys_right = static_cast<int>(static_cast<int64_t>(num_keys_right) *
                                    num_keys_actual / num_keys_desired);
  ARROW_DCHECK(num_keys_right >= num_keys_common);
  num_keys_left = num_keys_actual - num_keys_right + num_keys_common;
  if (num_keys_left == 0) {
    ARROW_DCHECK(num_keys_common == 0 && num_keys_right > 0);
    ++num_keys_left;
    ++num_keys_common;
  }
  if (num_keys_right == 0) {
    ARROW_DCHECK(num_keys_common == 0 && num_keys_left > 0);
    ++num_keys_right;
    ++num_keys_common;
  }
  ARROW_DCHECK(num_keys_left >= num_keys_common);
  ARROW_DCHECK(num_keys_left + num_keys_right - num_keys_common == num_keys_actual);

  key_id_l->resize(num_rows_l);
  for (int i = 0; i < num_rows_l; ++i) {
    (*key_id_l)[i] = rng.from_range(0, num_keys_left - 1);
    if ((*key_id_l)[i] >= num_keys_common) {
      (*key_id_l)[i] += num_keys_right - num_keys_common;
    }
  }

  key_id_r->resize(num_rows_r);
  for (int i = 0; i < num_rows_r; ++i) {
    (*key_id_r)[i] = rng.from_range(0, num_keys_right - 1);
  }

  std::vector<std::shared_ptr<Array>> key_l;
  std::vector<std::shared_ptr<Array>> key_r;
  TakeUsingVector(ctx, keys, *key_id_l, &key_l);
  TakeUsingVector(ctx, keys, *key_id_r, &key_r);
  std::vector<std::shared_ptr<Array>> payload_l =
      GenRandomRecords(rng, payload_left_types.data_types, num_rows_l);
  std::vector<std::shared_ptr<Array>> payload_r =
      GenRandomRecords(rng, payload_right_types.data_types, num_rows_r);

  left->resize(key_l.size() + payload_l.size());
  for (size_t i = 0; i < key_l.size(); ++i) {
    (*left)[i] = key_l[i];
  }
  for (size_t i = 0; i < payload_l.size(); ++i) {
    (*left)[key_l.size() + i] = payload_l[i];
  }
  right->resize(key_r.size() + payload_r.size());
  for (size_t i = 0; i < key_r.size(); ++i) {
    (*right)[i] = key_r[i];
  }
  for (size_t i = 0; i < payload_r.size(); ++i) {
    (*right)[key_r.size() + i] = payload_r[i];
  }
}

std::vector<std::shared_ptr<Array>> ConstructJoinOutputFromRowIds(
    ExecContext* ctx, const std::vector<int32_t>& row_ids_l,
    const std::vector<int32_t>& row_ids_r, const std::vector<std::shared_ptr<Array>>& l,
    const std::vector<std::shared_ptr<Array>>& r,
    const std::vector<int>& shuffle_output_l, const std::vector<int>& shuffle_output_r) {
  std::vector<std::shared_ptr<Array>> full_output_l;
  std::vector<std::shared_ptr<Array>> full_output_r;
  TakeUsingVector(ctx, l, row_ids_l, &full_output_l);
  TakeUsingVector(ctx, r, row_ids_r, &full_output_r);
  std::vector<std::shared_ptr<Array>> result;
  result.resize(shuffle_output_l.size() + shuffle_output_r.size());
  for (size_t i = 0; i < shuffle_output_l.size(); ++i) {
    result[i] = full_output_l[shuffle_output_l[i]];
  }
  for (size_t i = 0; i < shuffle_output_r.size(); ++i) {
    result[shuffle_output_l.size() + i] = full_output_r[shuffle_output_r[i]];
  }
  return result;
}

BatchesWithSchema TableToBatches(Random64Bit& rng, int num_batches,
                                 const std::vector<std::shared_ptr<Array>>& table,
                                 const std::string& column_name_prefix) {
  BatchesWithSchema result;

  std::vector<std::shared_ptr<Field>> fields;
  fields.resize(table.size());
  for (size_t i = 0; i < table.size(); ++i) {
    fields[i] = std::make_shared<Field>(column_name_prefix + std::to_string(i),
                                        table[i]->type(), true);
  }
  result.schema = std::make_shared<Schema>(std::move(fields));

  int64_t length = table[0]->length();
  num_batches = std::min(num_batches, static_cast<int>(length));

  std::vector<int64_t> batch_offsets;
  batch_offsets.push_back(0);
  batch_offsets.push_back(length);
  std::unordered_set<int64_t> batch_offset_set;
  for (int i = 0; i < num_batches - 1; ++i) {
    for (;;) {
      int64_t offset = rng.from_range(static_cast<int64_t>(1), length - 1);
      if (batch_offset_set.find(offset) == batch_offset_set.end()) {
        batch_offset_set.insert(offset);
        batch_offsets.push_back(offset);
        break;
      }
    }
  }
  std::sort(batch_offsets.begin(), batch_offsets.end());

  for (int i = 0; i < num_batches; ++i) {
    int64_t batch_offset = batch_offsets[i];
    int64_t batch_length = batch_offsets[i + 1] - batch_offsets[i];
    ExecBatch batch({}, batch_length);
    batch.values.resize(table.size());
    for (size_t col = 0; col < table.size(); ++col) {
      batch.values[col] = table[col]->data()->Slice(batch_offset, batch_length);
    }
    result.batches.push_back(batch);
  }

  return result;
}

// -1 in result means outputting all corresponding fields as nulls
//
void HashJoinSimpleInt(JoinType join_type, const std::vector<int32_t>& l,
                       const std::vector<bool>& null_in_key_l,
                       const std::vector<int32_t>& r,
                       const std::vector<bool>& null_in_key_r,
                       std::vector<int32_t>* result_l, std::vector<int32_t>* result_r,
                       int64_t output_length_limit, bool* length_limit_reached) {
  *length_limit_reached = false;

  bool switch_sides = false;
  switch (join_type) {
    case JoinType::RIGHT_SEMI:
      join_type = JoinType::LEFT_SEMI;
      switch_sides = true;
      break;
    case JoinType::RIGHT_ANTI:
      join_type = JoinType::LEFT_ANTI;
      switch_sides = true;
      break;
    case JoinType::RIGHT_OUTER:
      join_type = JoinType::LEFT_OUTER;
      switch_sides = true;
      break;
    default:
      break;
  }
  const std::vector<int32_t>& build = switch_sides ? l : r;
  const std::vector<int32_t>& probe = switch_sides ? r : l;
  const std::vector<bool>& null_in_key_build =
      switch_sides ? null_in_key_l : null_in_key_r;
  const std::vector<bool>& null_in_key_probe =
      switch_sides ? null_in_key_r : null_in_key_l;
  std::vector<int32_t>* result_build = switch_sides ? result_l : result_r;
  std::vector<int32_t>* result_probe = switch_sides ? result_r : result_l;

  std::unordered_multimap<int64_t, int64_t> map_build;
  for (size_t i = 0; i < build.size(); ++i) {
    map_build.insert(std::make_pair(build[i], i));
  }
  std::vector<bool> match_build;
  match_build.resize(build.size());
  for (size_t i = 0; i < build.size(); ++i) {
    match_build[i] = false;
  }

  for (int32_t i = 0; i < static_cast<int32_t>(probe.size()); ++i) {
    std::vector<int32_t> match_probe;
    if (!null_in_key_probe[i]) {
      auto range = map_build.equal_range(probe[i]);
      for (auto it = range.first; it != range.second; ++it) {
        if (!null_in_key_build[it->second]) {
          match_probe.push_back(static_cast<int32_t>(it->second));
          match_build[it->second] = true;
        }
      }
    }
    switch (join_type) {
      case JoinType::LEFT_SEMI:
        if (!match_probe.empty()) {
          result_probe->push_back(i);
          result_build->push_back(-1);
        }
        break;
      case JoinType::LEFT_ANTI:
        if (match_probe.empty()) {
          result_probe->push_back(i);
          result_build->push_back(-1);
        }
        break;
      case JoinType::INNER:
        for (size_t j = 0; j < match_probe.size(); ++j) {
          result_probe->push_back(i);
          result_build->push_back(match_probe[j]);
        }
        break;
      case JoinType::LEFT_OUTER:
      case JoinType::FULL_OUTER:
        if (match_probe.empty()) {
          result_probe->push_back(i);
          result_build->push_back(-1);
        } else {
          for (size_t j = 0; j < match_probe.size(); ++j) {
            result_probe->push_back(i);
            result_build->push_back(match_probe[j]);
          }
        }
        break;
      default:
        ARROW_DCHECK(false);
        break;
    }

    if (static_cast<int64_t>(result_probe->size()) >= output_length_limit) {
      *length_limit_reached = true;
      return;
    }
  }

  if (join_type == JoinType::FULL_OUTER) {
    for (int32_t i = 0; i < static_cast<int32_t>(build.size()); ++i) {
      if (!match_build[i]) {
        result_probe->push_back(-1);
        result_build->push_back(i);
      }
    }
  }
}

std::vector<int> GenShuffle(Random64Bit& rng, int length) {
  std::vector<int> shuffle(length);
  std::iota(shuffle.begin(), shuffle.end(), 0);
  for (int i = 0; i < length * 2; ++i) {
    int from = rng.from_range(0, length - 1);
    int to = rng.from_range(0, length - 1);
    if (from != to) {
      std::swap(shuffle[from], shuffle[to]);
    }
  }
  return shuffle;
}

void GenJoinFieldRefs(Random64Bit& rng, int num_key_fields, bool no_output,
                      const std::vector<std::shared_ptr<Array>>& original_input,
                      const std::string& field_name_prefix,
                      std::vector<std::shared_ptr<Array>>* new_input,
                      std::vector<FieldRef>* keys, std::vector<FieldRef>* output,
                      std::vector<int>* output_field_ids) {
  // Permute input
  std::vector<int> shuffle = GenShuffle(rng, static_cast<int>(original_input.size()));
  new_input->resize(original_input.size());
  for (size_t i = 0; i < original_input.size(); ++i) {
    (*new_input)[i] = original_input[shuffle[i]];
  }

  // Compute key field refs
  keys->resize(num_key_fields);
  for (size_t i = 0; i < shuffle.size(); ++i) {
    if (shuffle[i] < num_key_fields) {
      bool use_by_name_ref = (rng.from_range(0, 1) == 0);
      if (use_by_name_ref) {
        (*keys)[shuffle[i]] = FieldRef(field_name_prefix + std::to_string(i));
      } else {
        (*keys)[shuffle[i]] = FieldRef(static_cast<int>(i));
      }
    }
  }

  // Compute output field refs
  if (!no_output) {
    int num_output = rng.from_range(1, static_cast<int>(original_input.size() + 1));
    output_field_ids->resize(num_output);
    output->resize(num_output);
    for (int i = 0; i < num_output; ++i) {
      int col_id = rng.from_range(0, static_cast<int>(original_input.size() - 1));
      (*output_field_ids)[i] = col_id;
      (*output)[i] = (rng.from_range(0, 1) == 0)
                         ? FieldRef(field_name_prefix + std::to_string(col_id))
                         : FieldRef(col_id);
    }
  }
}

std::shared_ptr<Table> HashJoinSimple(
    ExecContext* ctx, JoinType join_type, const std::vector<JoinKeyCmp>& cmp,
    int num_key_fields, const std::vector<int32_t>& key_id_l,
    const std::vector<int32_t>& key_id_r,
    const std::vector<std::shared_ptr<Array>>& original_l,
    const std::vector<std::shared_ptr<Array>>& original_r,
    const std::vector<std::shared_ptr<Array>>& l,
    const std::vector<std::shared_ptr<Array>>& r, const std::vector<int>& output_ids_l,
    const std::vector<int>& output_ids_r, int64_t output_length_limit,
    bool* length_limit_reached) {
  std::vector<std::shared_ptr<Array>> key_l(num_key_fields);
  std::vector<std::shared_ptr<Array>> key_r(num_key_fields);
  for (int i = 0; i < num_key_fields; ++i) {
    key_l[i] = original_l[i];
    key_r[i] = original_r[i];
  }
  std::vector<bool> null_key_l = NullInKey(cmp, key_l);
  std::vector<bool> null_key_r = NullInKey(cmp, key_r);

  std::vector<int32_t> row_ids_l;
  std::vector<int32_t> row_ids_r;
  HashJoinSimpleInt(join_type, key_id_l, null_key_l, key_id_r, null_key_r, &row_ids_l,
                    &row_ids_r, output_length_limit, length_limit_reached);

  std::vector<std::shared_ptr<Array>> result = ConstructJoinOutputFromRowIds(
      ctx, row_ids_l, row_ids_r, l, r, output_ids_l, output_ids_r);

  std::vector<std::shared_ptr<Field>> fields(result.size());
  for (size_t i = 0; i < result.size(); ++i) {
    fields[i] = std::make_shared<Field>("a" + std::to_string(i), result[i]->type(), true);
  }
  std::shared_ptr<Schema> schema = std::make_shared<Schema>(std::move(fields));
  return Table::Make(schema, result, result[0]->length());
}

void HashJoinWithExecPlan(Random64Bit& rng, bool parallel,
                          const HashJoinNodeOptions& join_options,
                          const std::shared_ptr<Schema>& output_schema,
                          const std::vector<std::shared_ptr<Array>>& l,
                          const std::vector<std::shared_ptr<Array>>& r, int num_batches_l,
                          int num_batches_r, std::shared_ptr<Table>* output) {
  auto exec_ctx = arrow::internal::make_unique<ExecContext>(
      default_memory_pool(), parallel ? arrow::internal::GetCpuThreadPool() : nullptr);

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make(exec_ctx.get()));

  Declaration join{"hashjoin", join_options};

  // add left source
  BatchesWithSchema l_batches = TableToBatches(rng, num_batches_l, l, "l_");
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{l_batches.schema, l_batches.gen(parallel,
                                                                  /*slow=*/false)}});
  // add right source
  BatchesWithSchema r_batches = TableToBatches(rng, num_batches_r, r, "r_");
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r_batches.schema, r_batches.gen(parallel,
                                                                  /*slow=*/false)}});
  AsyncGenerator<util::optional<ExecBatch>> sink_gen;

  ASSERT_OK(Declaration::Sequence({join, {"sink", SinkNodeOptions{&sink_gen}}})
                .AddToPlan(plan.get()));

  ASSERT_FINISHES_OK_AND_ASSIGN(auto res, StartAndCollect(plan.get(), sink_gen));
  ASSERT_OK_AND_ASSIGN(*output, TableFromExecBatches(output_schema, res));
}

TEST(HashJoin, Random) {
  Random64Bit rng(42);

  int num_tests = 100;
  for (int test_id = 0; test_id < num_tests; ++test_id) {
    bool parallel = (rng.from_range(0, 1) == 1);
    auto exec_ctx = arrow::internal::make_unique<ExecContext>(
        default_memory_pool(), parallel ? arrow::internal::GetCpuThreadPool() : nullptr);

    // Constraints
    RandomDataTypeConstraints type_constraints;
    type_constraints.Default();
    // type_constraints.OnlyInt(1, true);
    constexpr int max_num_key_fields = 3;
    constexpr int max_num_payload_fields = 3;
    const char* join_type_names[] = {"LEFT_SEMI",   "RIGHT_SEMI", "LEFT_ANTI",
                                     "RIGHT_ANTI",  "INNER",      "LEFT_OUTER",
                                     "RIGHT_OUTER", "FULL_OUTER"};
    std::vector<JoinType> join_type_options{JoinType::LEFT_SEMI,   JoinType::RIGHT_SEMI,
                                            JoinType::LEFT_ANTI,   JoinType::RIGHT_ANTI,
                                            JoinType::INNER,       JoinType::LEFT_OUTER,
                                            JoinType::RIGHT_OUTER, JoinType::FULL_OUTER};
    constexpr int join_type_mask = 0xFF;
    // for INNER join only:
    // constexpr int join_type_mask = 0x10;
    std::vector<JoinKeyCmp> key_cmp_options{JoinKeyCmp::EQ, JoinKeyCmp::IS};
    constexpr int key_cmp_mask = 0x03;
    // for EQ only:
    // constexpr int key_cmp_mask = 0x01;
    constexpr int min_num_rows = 1;
    const int max_num_rows = parallel ? 20000 : 2000;
    constexpr int min_batch_size = 10;
    constexpr int max_batch_size = 100;

    // Generate list of key field data types
    int num_key_fields = rng.from_range(1, max_num_key_fields);
    RandomDataTypeVector key_types;
    for (int i = 0; i < num_key_fields; ++i) {
      key_types.AddRandom(rng, type_constraints);
    }

    // Generate lists of payload data types
    int num_payload_fields[2];
    RandomDataTypeVector payload_types[2];
    for (int i = 0; i < 2; ++i) {
      num_payload_fields[i] = rng.from_range(0, max_num_payload_fields);
      for (int j = 0; j < num_payload_fields[i]; ++j) {
        payload_types[i].AddRandom(rng, type_constraints);
      }
    }

    // Generate join type and comparison functions
    std::vector<JoinKeyCmp> key_cmp(num_key_fields);
    std::string key_cmp_str;
    for (int i = 0; i < num_key_fields; ++i) {
      for (;;) {
        int pos = rng.from_range(0, 1);
        if ((key_cmp_mask & (1 << pos)) > 0) {
          key_cmp[i] = key_cmp_options[pos];
          if (i > 0) {
            key_cmp_str += "_";
          }
          key_cmp_str += key_cmp[i] == JoinKeyCmp::EQ ? "EQ" : "IS";
          break;
        }
      }
    }
    JoinType join_type;
    std::string join_type_name;
    for (;;) {
      int pos = rng.from_range(0, 7);
      if ((join_type_mask & (1 << pos)) > 0) {
        join_type = join_type_options[pos];
        join_type_name = join_type_names[pos];
        break;
      }
    }

    // Generate input records
    int num_rows_l = rng.from_range(min_num_rows, max_num_rows);
    int num_rows_r = rng.from_range(min_num_rows, max_num_rows);
    int num_rows = std::min(num_rows_l, num_rows_r);
    int batch_size = rng.from_range(min_batch_size, max_batch_size);
    int num_keys = rng.from_range(std::max(1, num_rows / 10), num_rows);
    int num_keys_r = rng.from_range(std::max(1, num_keys / 2), num_keys);
    int num_keys_common = rng.from_range(std::max(1, num_keys_r / 2), num_keys_r);
    int num_keys_l = num_keys_common + (num_keys - num_keys_r);
    std::vector<int> key_id_vectors[2];
    std::vector<std::shared_ptr<Array>> input_arrays[2];
    GenRandomJoinTables(exec_ctx.get(), rng, num_rows_l, num_rows_r, num_keys_common,
                        num_keys_l, num_keys_r, key_types, payload_types[0],
                        payload_types[1], &(key_id_vectors[0]), &(key_id_vectors[1]),
                        &(input_arrays[0]), &(input_arrays[1]));
    std::vector<std::shared_ptr<Array>> shuffled_input_arrays[2];
    std::vector<FieldRef> key_fields[2];
    std::vector<FieldRef> output_fields[2];
    std::vector<int> output_field_ids[2];
    for (int i = 0; i < 2; ++i) {
      bool no_output = false;
      if (i == 0) {
        no_output =
            join_type == JoinType::RIGHT_SEMI || join_type == JoinType::RIGHT_ANTI;
      } else {
        no_output = join_type == JoinType::LEFT_SEMI || join_type == JoinType::LEFT_ANTI;
      }
      GenJoinFieldRefs(rng, num_key_fields, no_output, input_arrays[i],
                       std::string((i == 0) ? "l_" : "r_"), &(shuffled_input_arrays[i]),
                       &(key_fields[i]), &(output_fields[i]), &(output_field_ids[i]));
    }

    // Print test case parameters
    // print num_rows, batch_size, join_type, join_cmp
    std::cout << join_type_name << " " << key_cmp_str << " ";
    key_types.Print();
    std::cout << " num_rows_l = " << num_rows_l << " num_rows_r = " << num_rows_r
              << " batch size = " << batch_size
              << " parallel = " << (parallel ? "true" : "false");
    std::cout << std::endl;

    // Run reference join implementation
    std::vector<bool> null_in_key_vectors[2];
    for (int i = 0; i < 2; ++i) {
      null_in_key_vectors[i] = NullInKey(key_cmp, input_arrays[i]);
    }
    int64_t output_length_limit = 100000;
    bool length_limit_reached = false;
    std::shared_ptr<Table> output_rows_ref = HashJoinSimple(
        exec_ctx.get(), join_type, key_cmp, num_key_fields, key_id_vectors[0],
        key_id_vectors[1], input_arrays[0], input_arrays[1], shuffled_input_arrays[0],
        shuffled_input_arrays[1], output_field_ids[0], output_field_ids[1],
        output_length_limit, &length_limit_reached);
    if (length_limit_reached) {
      continue;
    }

    // Run tested join implementation
    HashJoinNodeOptions join_options{join_type,        key_fields[0],    key_fields[1],
                                     output_fields[0], output_fields[1], key_cmp};
    std::vector<std::shared_ptr<Field>> output_schema_fields;
    for (int i = 0; i < 2; ++i) {
      for (size_t col = 0; col < output_fields[i].size(); ++col) {
        output_schema_fields.push_back(std::make_shared<Field>(
            std::string((i == 0) ? "l_" : "r_") + std::to_string(col),
            shuffled_input_arrays[i][output_field_ids[i][col]]->type(), true));
      }
    }
    std::shared_ptr<Schema> output_schema =
        std::make_shared<Schema>(std::move(output_schema_fields));
    std::shared_ptr<Table> output_rows_test;
    HashJoinWithExecPlan(rng, parallel, join_options, output_schema,
                         shuffled_input_arrays[0], shuffled_input_arrays[1],
                         static_cast<int>(BitUtil::CeilDiv(num_rows_l, batch_size)),
                         static_cast<int>(BitUtil::CeilDiv(num_rows_r, batch_size)),
                         &output_rows_test);

    // Compare results
    AssertTablesEqual(output_rows_ref, output_rows_test);
  }
}

}  // namespace compute
}  // namespace arrow
