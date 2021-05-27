#include "gandiva/projector_object_cache.h"

namespace gandiva {

std::shared_ptr<Cache<ProjectorCacheKey, std::shared_ptr<llvm::MemoryBuffer>>> ProjectorObjectCache::projector_cache_;

void ProjectorObjectCache::SetCache(std::shared_ptr<Cache<ProjectorCacheKey, std::shared_ptr<llvm::MemoryBuffer>>>& cache) {
  ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: Called SetCache()";
  projector_cache_ = cache;
  std::string cache_log = projector_cache_->toString();
  ARROW_LOG(INFO) << cache_log;
}

void ProjectorObjectCache::SetKey(std::unique_ptr<ProjectorCacheKey>& key){
  ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: Called SetKey()";
  projector_key_ = std::move(key);
  auto key_log = *projector_key_;
  ARROW_LOG(INFO) << key_log.ToString();
}

}
