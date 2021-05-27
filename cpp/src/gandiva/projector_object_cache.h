#pragma once

#include <llvm/Support/MemoryBuffer.h>
#include "llvm/ExecutionEngine/ObjectCache.h"
#include "llvm/IR/Module.h"
#include "gandiva/cache.h"
#include "gandiva/projector.h"

namespace gandiva {

class ProjectorObjectCache : public llvm::ObjectCache {
 public:
  //ProjectorObjectCache(SchemaPtr schema, const ExpressionVector& exprs,
  //    SelectionVector::Mode selection_vector_mode,
  //    std::shared_ptr<Configuration> configuration) :
  //   projector_key_(schema, configuration, exprs, selection_vector_mode){
  //  ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: Constructed the Projector Object Cache";
  //}
  ProjectorObjectCache(std::shared_ptr<Cache<ProjectorCacheKey, std::shared_ptr<llvm::MemoryBuffer>>>& cache,
                       std::shared_ptr<ProjectorCacheKey>& key){
    ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: Constructed the Projector Object Cache";
    projector_cache_ = cache;
    projector_key_ = key;
    std::string cache_log = projector_cache_->toString();
    ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: " << cache_log;
  };

  ~ProjectorObjectCache() {
    ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: Destructed the Projector Object Cache";
  }

  void notifyObjectCompiled(const llvm::Module* M, llvm::MemoryBufferRef Obj){
    ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: LLVM called notifyObjectCompiled() to compile the ObjectCode";
    std::unique_ptr<llvm::MemoryBuffer> obj_buffer = llvm::MemoryBuffer::getMemBufferCopy(Obj.getBuffer(), Obj.getBufferIdentifier());
    std::shared_ptr<llvm::MemoryBuffer> obj_code = std::move(obj_buffer);
    projector_cache_->PutObjectCode(*projector_key_.get(), obj_code);
  };

  std::unique_ptr<llvm::MemoryBuffer> getObject(const llvm::Module* M){
    ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: LLVM called getObject() to get the cached ObjectCode";
    std::shared_ptr<llvm::MemoryBuffer> cached_obj = projector_cache_->GetObjectCode(*projector_key_.get());
    if(cached_obj == nullptr) {
      ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: LLVM cached ObjectCode was NOT found, need to compile it.";
      return nullptr;
    }
    std::unique_ptr<llvm::MemoryBuffer> cached_buffer = cached_obj->getMemBufferCopy(cached_obj->getBuffer(), cached_obj->getBufferIdentifier());
    ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: LLVM cached ObjectCode was found, NO need to compile it.";
    return cached_buffer;

  };

  void SetCache(std::shared_ptr<Cache<ProjectorCacheKey, std::shared_ptr<llvm::MemoryBuffer>>>&);
  void SetKey(std::shared_ptr<ProjectorCacheKey>&);

  std::shared_ptr<Cache<ProjectorCacheKey, std::shared_ptr<llvm::MemoryBuffer>>> GetCache();
  std::shared_ptr<ProjectorCacheKey> GetKey();
 private:
  std::shared_ptr<ProjectorCacheKey> projector_key_;
  static std::shared_ptr<Cache<ProjectorCacheKey, std::shared_ptr<llvm::MemoryBuffer>>> projector_cache_;
};
}  // namespace gandiva