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

#include "gandiva/gdv_function_stubs.h"

#include <string>
#include <vector>

#include "arrow/util/value_parsing.h"
#include "gandiva/hash_utils.h"
#include "gandiva/engine.h"
#include "gandiva/exported_funcs.h"
#include "gandiva/in_holder.h"
#include "gandiva/like_holder.h"
#include "gandiva/random_generator_holder.h"
#include "gandiva/to_date_holder.h"


/// Stub functions that can be accessed from LLVM or the pre-compiled library.

extern "C" {

bool gdv_fn_like_utf8_utf8(int64_t ptr, const char* data, int data_len,
                           const char* pattern, int pattern_len) {
  gandiva::LikeHolder* holder = reinterpret_cast<gandiva::LikeHolder*>(ptr);
  return (*holder)(std::string(data, data_len));
}

double gdv_fn_random(int64_t ptr) {
  gandiva::RandomGeneratorHolder* holder =
      reinterpret_cast<gandiva::RandomGeneratorHolder*>(ptr);
  return (*holder)();
}

double gdv_fn_random_with_seed(int64_t ptr, int32_t seed, bool seed_validity) {
  gandiva::RandomGeneratorHolder* holder =
      reinterpret_cast<gandiva::RandomGeneratorHolder*>(ptr);
  return (*holder)();
}

int64_t gdv_fn_to_date_utf8_utf8(int64_t context_ptr, int64_t holder_ptr,
                                 const char* data, int data_len, bool in1_validity,
                                 const char* pattern, int pattern_len, bool in2_validity,
                                 bool* out_valid) {
  gandiva::ExecutionContext* context =
      reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  gandiva::ToDateHolder* holder = reinterpret_cast<gandiva::ToDateHolder*>(holder_ptr);
  return (*holder)(context, data, data_len, in1_validity, out_valid);
}

int64_t gdv_fn_to_date_utf8_utf8_int32(int64_t context_ptr, int64_t holder_ptr,
                                       const char* data, int data_len, bool in1_validity,
                                       const char* pattern, int pattern_len,
                                       bool in2_validity, int32_t suppress_errors,
                                       bool in3_validity, bool* out_valid) {
  gandiva::ExecutionContext* context =
      reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  gandiva::ToDateHolder* holder = reinterpret_cast<gandiva::ToDateHolder*>(holder_ptr);
  return (*holder)(context, data, data_len, in1_validity, out_valid);
}

bool gdv_fn_in_expr_lookup_int32(int64_t ptr, int32_t value, bool in_validity) {
  if (!in_validity) {
    return false;
  }
  gandiva::InHolder<int32_t>* holder = reinterpret_cast<gandiva::InHolder<int32_t>*>(ptr);
  return holder->HasValue(value);
}

bool gdv_fn_in_expr_lookup_int64(int64_t ptr, int64_t value, bool in_validity) {
  if (!in_validity) {
    return false;
  }
  gandiva::InHolder<int64_t>* holder = reinterpret_cast<gandiva::InHolder<int64_t>*>(ptr);
  return holder->HasValue(value);
}

bool gdv_fn_in_expr_lookup_utf8(int64_t ptr, const char* data, int data_len,
                                bool in_validity) {
  if (!in_validity) {
    return false;
  }
  gandiva::InHolder<std::string>* holder =
      reinterpret_cast<gandiva::InHolder<std::string>*>(ptr);
  return holder->HasValue(arrow::util::string_view(data, data_len));
}

int32_t gdv_fn_populate_varlen_vector(int64_t context_ptr, int8_t* data_ptr,
                                      int32_t* offsets, int64_t slot,
                                      const char* entry_buf, int32_t entry_len) {
  auto buffer = reinterpret_cast<arrow::ResizableBuffer*>(data_ptr);
  int32_t offset = static_cast<int32_t>(buffer->size());

  // This also sets the size in the buffer.
  auto status = buffer->Resize(offset + entry_len, false /*shrink*/);
  if (!status.ok()) {
    gandiva::ExecutionContext* context =
        reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);

    context->set_error_msg(status.message().c_str());
    return -1;
  }

  // append the new entry.
  memcpy(buffer->mutable_data() + offset, entry_buf, entry_len);

  // update offsets buffer.
  offsets[slot] = offset;
  offsets[slot + 1] = offset + entry_len;
  return 0;
}

#define SHA256_HASH_FUNCTION(TYPE)                                                       \
  GANDIVA_EXPORT                                                                         \
  const char *gdv_fn_sha256_from_##TYPE(int64_t context, gdv_##TYPE value,               \
                                        bool validity, u_int32_t *out_length) {          \
    if(!validity){                                                                       \
      *out_length = 0;                                                                   \
      return "";                                                                         \
    }                                                                                    \
                                                                                         \
    auto value_as_long = gandiva::HashUtils::double_to_long((double)value);              \
                                                                                         \
    const char *result = gandiva::HashUtils::hash_using_SHA256(context,                  \
                                                               &value_as_long,           \
                                                               sizeof(value_as_long),    \
                                                               out_length);              \
                                                                                         \
    return result;                                                                       \
  }                                                                                      \

#define SHA256_HASH_FUNCTION_BUF(TYPE)                                                       \
  GANDIVA_EXPORT                                                                             \
  const char *gdv_fn_sha256_from_##TYPE(int64_t context,                                     \
                                        gdv_##TYPE value,                                    \
                                        int32_t value_length,                                \
                                        bool value_validity,                                 \
                                        u_int32_t *out_length) {                             \
    if(!value_validity){                                                                     \
      *out_length = 0;                                                                       \
      return "";                                                                             \
    }                                                                                        \
                                                                                             \
    return gandiva::HashUtils::hash_using_SHA256(context, value, value_length, out_length);  \
  }

GANDIVA_EXPORT
const char* gdv_fn_sha128_from_numeric(int64_t context, double value){
  uint64_t value_as_long = gandiva::HashUtils::double_to_long(value);

  return gandiva::HashUtils::hash_using_SHA128(context, &value_as_long, sizeof(value_as_long), nullptr);
}

GANDIVA_EXPORT
const char* gdv_fn_hash_sha128_from_string(int64_t context, const char* value, uint64_t value_length){
  return gandiva::HashUtils::hash_using_SHA128(context, value, value_length, nullptr);
}

// Expand inner macro for all numeric types.
#define NUMERIC_BOOL_DATE_TYPES(INNER) \
  INNER(int8)                          \
  INNER(int16)                         \
  INNER(int32)                         \
  INNER(int64)                         \
  INNER(uint8)                         \
  INNER(uint16)                        \
  INNER(uint32)                        \
  INNER(uint64)                        \
  INNER(float32)                       \
  INNER(float64)                       \
  INNER(boolean)                       \
  INNER(date64)                        \
  INNER(date32)                        \
  INNER(time32)                        \
  INNER(timestamp)


// Expand inner macro for all numeric types.
#define VAR_LEN_TYPES(INNER) \
  INNER(utf8)                \
  INNER(binary)


NUMERIC_BOOL_DATE_TYPES(SHA256_HASH_FUNCTION)
VAR_LEN_TYPES(SHA256_HASH_FUNCTION_BUF)

#undef NUMERIC_BOOL_DATE_TYPES
#undef VAR_LEN_TYPES

int32_t gdv_fn_dec_from_string(int64_t context, const char* in, int32_t in_length,
                               int32_t* precision_from_str, int32_t* scale_from_str,
                               int64_t* dec_high_from_str, uint64_t* dec_low_from_str) {
  arrow::Decimal128 dec;
  auto status = arrow::Decimal128::FromString(std::string(in, in_length), &dec,
                                              precision_from_str, scale_from_str);
  if (!status.ok()) {
    gdv_fn_context_set_error_msg(context, status.message().data());
    return -1;
  }
  *dec_high_from_str = dec.high_bits();
  *dec_low_from_str = dec.low_bits();
  return 0;
}

char* gdv_fn_dec_to_string(int64_t context, int64_t x_high, uint64_t x_low,
                           int32_t x_scale, int32_t* dec_str_len) {
  arrow::Decimal128 dec(arrow::BasicDecimal128(x_high, x_low));
  std::string dec_str = dec.ToString(x_scale);
  *dec_str_len = static_cast<int32_t>(dec_str.length());
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *dec_str_len));
  if (ret == nullptr) {
    std::string err_msg = "Could not allocate memory for string: " + dec_str;
    gdv_fn_context_set_error_msg(context, err_msg.data());
    return nullptr;
  }
  memcpy(ret, dec_str.data(), *dec_str_len);
  return ret;
}

#define CAST_NUMERIC_FROM_STRING(OUT_TYPE, ARROW_TYPE, TYPE_NAME)                    \
  GANDIVA_EXPORT                                                                     \
  OUT_TYPE gdv_fn_cast##TYPE_NAME##_utf8(int64_t context, const char* data,          \
                                         int32_t len) {                              \
    OUT_TYPE val = 0;                                                                \
    /* trim leading and trailing spaces */                                           \
    int32_t trimmed_len;                                                             \
    int32_t start = 0, end = len - 1;                                                \
    while (start <= end && data[start] == ' ') {                                     \
      ++start;                                                                       \
    }                                                                                \
    while (end >= start && data[end] == ' ') {                                       \
      --end;                                                                         \
    }                                                                                \
    trimmed_len = end - start + 1;                                                   \
    const char* trimmed_data = data + start;                                         \
    if (!arrow::internal::ParseValue<ARROW_TYPE>(trimmed_data, trimmed_len, &val)) { \
      std::string err =                                                              \
          "Failed to cast the string " + std::string(data, len) + " to " #OUT_TYPE;  \
      gdv_fn_context_set_error_msg(context, err.c_str());                            \
    }                                                                                \
    return val;                                                                      \
  }

CAST_NUMERIC_FROM_STRING(int32_t, arrow::Int32Type, INT)
CAST_NUMERIC_FROM_STRING(int64_t, arrow::Int64Type, BIGINT)
CAST_NUMERIC_FROM_STRING(float, arrow::FloatType, FLOAT4)
CAST_NUMERIC_FROM_STRING(double, arrow::DoubleType, FLOAT8)

#undef CAST_NUMERIC_FROM_STRING
}

namespace gandiva {

void ExportedStubFunctions::AddMappings(Engine* engine) const {
  std::vector<llvm::Type*> args;
  auto types = engine->types();

  // gdv_fn_dec_from_string
  args = {
      types->i64_type(),      // context
      types->i8_ptr_type(),   // const char* in
      types->i32_type(),      // int32_t in_length
      types->i32_ptr_type(),  // int32_t* precision_from_str
      types->i32_ptr_type(),  // int32_t* scale_from_str
      types->i64_ptr_type(),  // int64_t* dec_high_from_str
      types->i64_ptr_type(),  // int64_t* dec_low_from_str
  };

  engine->AddGlobalMappingForFunc("gdv_fn_dec_from_string",
                                  types->i32_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_dec_from_string));

  // gdv_fn_dec_to_string
  args = {
      types->i64_type(),      // context
      types->i64_type(),      // int64_t x_high
      types->i64_type(),      // int64_t x_low
      types->i32_type(),      // int32_t x_scale
      types->i64_ptr_type(),  // int64_t* dec_str_len
  };

  engine->AddGlobalMappingForFunc("gdv_fn_dec_to_string",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_dec_to_string));

  // gdv_fn_like_utf8_utf8
  args = {types->i64_type(),     // int64_t ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type(),     // int data_len
          types->i8_ptr_type(),  // const char* pattern
          types->i32_type()};    // int pattern_len

  engine->AddGlobalMappingForFunc("gdv_fn_like_utf8_utf8",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_like_utf8_utf8));

  // gdv_fn_to_date_utf8_utf8
  args = {types->i64_type(),                   // int64_t execution_context
          types->i64_type(),                   // int64_t holder_ptr
          types->i8_ptr_type(),                // const char* data
          types->i32_type(),                   // int data_len
          types->i1_type(),                    // bool in1_validity
          types->i8_ptr_type(),                // const char* pattern
          types->i32_type(),                   // int pattern_len
          types->i1_type(),                    // bool in2_validity
          types->ptr_type(types->i8_type())};  // bool* out_valid

  engine->AddGlobalMappingForFunc("gdv_fn_to_date_utf8_utf8",
                                  types->i64_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_to_date_utf8_utf8));

  // gdv_fn_to_date_utf8_utf8_int32
  args = {types->i64_type(),                   // int64_t execution_context
          types->i64_type(),                   // int64_t holder_ptr
          types->i8_ptr_type(),                // const char* data
          types->i32_type(),                   // int data_len
          types->i1_type(),                    // bool in1_validity
          types->i8_ptr_type(),                // const char* pattern
          types->i32_type(),                   // int pattern_len
          types->i1_type(),                    // bool in2_validity
          types->i32_type(),                   // int32_t suppress_errors
          types->i1_type(),                    // bool in3_validity
          types->ptr_type(types->i8_type())};  // bool* out_valid

  engine->AddGlobalMappingForFunc(
      "gdv_fn_to_date_utf8_utf8_int32", types->i64_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_to_date_utf8_utf8_int32));

  // gdv_fn_in_expr_lookup_int32
  args = {types->i64_type(),  // int64_t in holder ptr
          types->i32_type(),  // int32 value
          types->i1_type()};  // bool in_validity

  engine->AddGlobalMappingForFunc("gdv_fn_in_expr_lookup_int32",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_in_expr_lookup_int32));

  // gdv_fn_in_expr_lookup_int64
  args = {types->i64_type(),  // int64_t in holder ptr
          types->i64_type(),  // int64 value
          types->i1_type()};  // bool in_validity

  engine->AddGlobalMappingForFunc("gdv_fn_in_expr_lookup_int64",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_in_expr_lookup_int64));

  // gdv_fn_in_expr_lookup_utf8
  args = {types->i64_type(),     // int64_t in holder ptr
          types->i8_ptr_type(),  // const char* value
          types->i32_type(),     // int value_len
          types->i1_type()};     // bool in_validity

  engine->AddGlobalMappingForFunc("gdv_fn_in_expr_lookup_utf8",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_in_expr_lookup_utf8));

  // gdv_fn_populate_varlen_vector
  args = {types->i64_type(),      // int64_t execution_context
          types->i8_ptr_type(),   // int8_t* data ptr
          types->i32_ptr_type(),  // int32_t* offsets ptr
          types->i64_type(),      // int64_t slot
          types->i8_ptr_type(),   // const char* entry_buf
          types->i32_type()};     // int32_t entry__len

  engine->AddGlobalMappingForFunc("gdv_fn_populate_varlen_vector",
                                  types->i32_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_populate_varlen_vector));

  // gdv_fn_random
  args = {types->i64_type()};
  engine->AddGlobalMappingForFunc("gdv_fn_random", types->double_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_random));

  args = {types->i64_type(), types->i32_type(), types->i1_type()};
  engine->AddGlobalMappingForFunc("gdv_fn_random_with_seed", types->double_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_random_with_seed));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castINT_utf8", types->i32_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castINT_utf8));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castBIGINT_utf8", types->i64_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castBIGINT_utf8));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castFLOAT4_utf8", types->float_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castFLOAT4_utf8));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castFLOAT8_utf8", types->double_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castFLOAT8_utf8));

  // gdv_fn_sha256_from_int8
  args = {
      types->i64_type(),       // context
      types->i8_type(),        // value
      types->i1_type(),        // validity
      types->i32_ptr_type()    // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_int8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_int8));

  // gdv_fn_sha256_from_int16
  args = {
      types->i64_type(),       // context
      types->i16_type(),       // value
      types->i1_type(),        // validity
      types->i32_ptr_type()    // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_int16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_int16));

  // gdv_fn_sha256_from_int32
  args = {
      types->i64_type(),       // context
      types->i32_type(),       // value
      types->i1_type(),        // validity
      types->i32_ptr_type()    // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_int32));

  // gdv_fn_sha256_from_int32
  args = {
      types->i64_type(),       // context
      types->i64_type(),       // value
      types->i1_type(),        // validity
      types->i32_ptr_type()    // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_int64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_int64));

  // gdv_fn_sha256_from_uint8
  args = {
      types->i64_type(),       // context
      types->i8_type(),        // value
      types->i1_type(),        // validity
      types->i32_ptr_type()    // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_uint8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_uint8));

  // gdv_fn_sha256_from_uint16
  args = {
      types->i64_type(),       // context
      types->i16_type(),      // value
      types->i1_type(),        // validity
      types->i32_ptr_type()    // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_uint16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_uint16));

  // gdv_fn_sha256_from_uint32
  args = {
      types->i64_type(),       // context
      types->i32_type(),      // value
      types->i1_type(),        // validity
      types->i32_ptr_type()    // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_uint32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_uint32));

  // gdv_fn_sha256_from_uint64
  args = {
      types->i64_type(),       // context
      types->i64_type(),      // value
      types->i1_type(),        // validity
      types->i32_ptr_type()    // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_uint64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_uint64));

  // gdv_fn_sha256_from_float32
  args = {
      types->i64_type(),      // context
      types->float_type(),    // value
      types->i1_type(),        // validity
      types->i32_ptr_type()    // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_float32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_float32));

  // gdv_fn_sha256_from_float64
  args = {
      types->i64_type(),      // context
      types->double_type(),    // value
      types->i1_type(),        // validity
      types->i32_ptr_type()    // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_float64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_float64));

  // gdv_fn_sha256_from_boolean
  args = {
      types->i64_type(),       // context
      types->i1_type(),    // value
      types->i1_type(),        // validity
      types->i32_ptr_type()    // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_boolean",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_boolean));

  // gdv_fn_sha256_from_date64
  args = {
      types->i64_type(),       // context
      types->i64_type(),    // value
      types->i1_type(),        // validity
      types->i32_ptr_type()    // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_date64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_date64));

  // gdv_fn_sha256_from_date32
  args = {
      types->i64_type(),       // context
      types->i32_type(),    // value
      types->i1_type(),        // validity
      types->i32_ptr_type()    // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_date32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_date32));

  // gdv_fn_sha256_from_time32
  args = {
      types->i64_type(),       // context
      types->i32_type(),    // value
      types->i1_type(),        // validity
      types->i32_ptr_type()    // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_time32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_time32));

  // gdv_fn_sha256_from_timestamp
  args = {
      types->i64_type(),       // context
      types->i64_type(),    // value
      types->i1_type(),        // validity
      types->i32_ptr_type()    // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_timestamp",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_timestamp));

  // gdv_fn_hash_sha256_from_utf8
  args = {
      types->i64_type(),      // context
      types->i8_ptr_type(),   // const char*
      types->i32_type(),      // value_length
      types->i1_type(),       // validity
      types->i32_ptr_type()       // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_utf8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_utf8));

  // gdv_fn_hash_sha256_from_binary
  args = {
      types->i64_type(),      // context
      types->i8_ptr_type(),   // const char*
      types->i32_type(),      // value_length
      types->i1_type(),       // validity
      types->i32_ptr_type()       // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha256_from_binary",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void *>(gdv_fn_sha256_from_binary));

}

}  // namespace gandiva
