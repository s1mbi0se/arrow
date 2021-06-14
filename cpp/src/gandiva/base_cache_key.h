#pragma once

#include <arrow/util/hash_util.h>
#include <stddef.h>

#include "gandiva/expression.h"
#include "gandiva/projector.h"
#include "gandiva/filter.h"

namespace gandiva {

class BaseCacheKey {
 public:

  BaseCacheKey(Expression& expr, std::string type) : type_(type) {
    static const int kSeedValue = 4;
    std::string expr_as_string = expr.ToString();
    size_t result_hash = kSeedValue;
    arrow::internal::hash_combine(result_hash, type);
    arrow::internal::hash_combine(result_hash, expr_as_string);
    hash_code_ = result_hash;
  };

  BaseCacheKey(ProjectorCacheKey& key, std::string type) : type_(type) {
    static const int kSeedValue = 4;
    size_t key_hash = key.Hash();
    size_t result_hash = kSeedValue;
    arrow::internal::hash_combine(result_hash, type);
    arrow::internal::hash_combine(result_hash, key_hash);
    hash_code_ = result_hash;
  };

  BaseCacheKey(FilterCacheKey& key, std::string type) : type_(type) {
    static const int kSeedValue = 4;
    size_t key_hash = key.Hash();
    size_t result_hash = kSeedValue;
    arrow::internal::hash_combine(result_hash, type);
    arrow::internal::hash_combine(result_hash, key_hash);
    hash_code_ = result_hash;
  };

  BaseCacheKey(std::shared_ptr<arrow::Schema> schema, std::shared_ptr<Expression> expr,
               std::string type) : type_(type) {
    static const int kSeedValue = 4;
    unsigned long int result_hash = kSeedValue;
    arrow::internal::hash_combine(result_hash, type);
    arrow::internal::hash_combine(result_hash, schema->ToString());
    arrow::internal::hash_combine(result_hash, expr->ToString());
    hash_code_ = result_hash;
  };

  size_t Hash() const{
    return hash_code_;
  }

  bool operator==(const BaseCacheKey& other) const {
    if (hash_code_ != other.hash_code_) {
      return false;
    }
    return true;
  };

  bool operator!=(const BaseCacheKey& other) const {
    return !(*this == other);
  }


 private:
  uint64_t hash_code_;
  std::string type_;

};

}
