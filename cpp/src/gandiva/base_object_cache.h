#pragma once

#include <llvm/Support/MemoryBuffer.h>
#include "llvm/ExecutionEngine/ObjectCache.h"
#include "llvm/IR/Module.h"
#include "gandiva/cache.h"
#include "gandiva/projector.h"
#include "gandiva/filter.h"

namespace gandiva {
template<class CacheKey>
class BaseObjectCache : public llvm::ObjectCache {
 public:
  BaseObjectCache(std::shared_ptr<Cache<CacheKey, std::shared_ptr<llvm::MemoryBuffer>>>& cache,
                       std::shared_ptr<CacheKey>& key){
    //ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: Constructed the Projector Object Cache";
    cache_ = cache;
    cache_key_ = key;
  };

  ~BaseObjectCache() {
    //ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: Destructed the Projector Object Cache";
  }

  void notifyObjectCompiled(const llvm::Module* M, llvm::MemoryBufferRef Obj){
    //ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: LLVM called notifyObjectCompiled() to compile the ObjectCode";
    std::unique_ptr<llvm::MemoryBuffer> obj_buffer = llvm::MemoryBuffer::getMemBufferCopy(Obj.getBuffer(), Obj.getBufferIdentifier());
    std::shared_ptr<llvm::MemoryBuffer> obj_code = std::move(obj_buffer);
    //ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: The size of the obj buffer code is " + std::to_string(obj_code->getBufferSize()) + " bytes";
    cache_->PutObjectCode(*cache_key_.get(), obj_code, obj_code->getBufferSize());
  };

  std::unique_ptr<llvm::MemoryBuffer> getObject(const llvm::Module* M){
    //ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: LLVM called getObject() to get the cached ObjectCode";
    std::shared_ptr<llvm::MemoryBuffer> cached_obj =
        cache_->GetObjectCode(*cache_key_.get());
    if(cached_obj == nullptr) {
      //ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: LLVM cached ObjectCode was NOT found, need to compile it.";
      return nullptr;
    }
    std::unique_ptr<llvm::MemoryBuffer> cached_buffer = cached_obj->getMemBufferCopy(cached_obj->getBuffer(), cached_obj->getBufferIdentifier());
    //ARROW_LOG(INFO) << "[OBJ-CACHE-LOG]: LLVM cached ObjectCode was found, NO need to compile it.";
    return cached_buffer;

  };

 private:
  std::shared_ptr<CacheKey> cache_key_;
  std::shared_ptr<Cache<CacheKey, std::shared_ptr<llvm::MemoryBuffer>>> cache_;
};
}  // namespace gandiva