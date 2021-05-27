#include "gandiva/projector_object_cache.h"

namespace gandiva {

std::shared_ptr<Cache<ProjectorCacheKey, std::shared_ptr<llvm::MemoryBuffer>>> ProjectorObjectCache::projector_cache_;

void ProjectorObjectCache::SetCache(std::shared_ptr<Cache<ProjectorCacheKey, std::shared_ptr<llvm::MemoryBuffer>>>& cache) {
  ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: Called SetCache()";
  projector_cache_ = cache;
  std::string cache_log = projector_cache_->toString();
  ARROW_LOG(INFO) << cache_log;
}

void ProjectorObjectCache::SetKey(std::shared_ptr<ProjectorCacheKey>& key){
  ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: Called SetKey()";
  auto key_log = key.get();
  ARROW_LOG(INFO) << key_log;
  projector_key_ = std::move(key);
  auto projector_key_log = projector_key_.get();
  ARROW_LOG(INFO) << projector_key_log;

  if(key_log == projector_key_log) {
    ARROW_LOG(INFO) << "[SET KEYS] BOTH KEYS ARE EQUALS";
  }
}

std::shared_ptr<Cache<ProjectorCacheKey, std::shared_ptr<llvm::MemoryBuffer>>> ProjectorObjectCache::GetCache(){
  return projector_cache_;
}

std::shared_ptr<ProjectorCacheKey> ProjectorObjectCache::GetKey(){
  /*auto projector_key_log = projector_key_.get();
  ARROW_LOG(INFO) << projector_key_log;
  std::unique_ptr<ProjectorCacheKey> key = std::move(projector_key_);
  auto key_log = key.get();
  ARROW_LOG(INFO) << key_log;
  if(key_log == projector_key_log) {
    ARROW_LOG(INFO) << "[GET KEYS] BOTH KEYS ARE EQUALS";
  }*/
  return projector_key_;
}

}
