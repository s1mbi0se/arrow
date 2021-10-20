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

#include "arrow/util/async_util.h"

#include <gtest/gtest.h>

#include "arrow/result.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace util {

class GatingDestroyable : public AsyncDestroyable {
 public:
  GatingDestroyable(Future<> close_future, bool* destroyed)
      : close_future_(std::move(close_future)), destroyed_(destroyed) {}
  ~GatingDestroyable() override { *destroyed_ = true; }

 protected:
  Future<> DoDestroy() override { return close_future_; }

 private:
  Future<> close_future_;
  bool* destroyed_;
};

template <typename Factory>
void TestAsyncDestroyable(Factory factory) {
  Future<> gate = Future<>::Make();
  bool destroyed = false;
  bool on_closed = false;
  {
    auto obj = factory(gate, &destroyed);
    obj->on_closed().AddCallback([&](const Status& st) { on_closed = true; });
    ASSERT_FALSE(destroyed);
  }
  ASSERT_FALSE(destroyed);
  ASSERT_FALSE(on_closed);
  gate.MarkFinished();
  ASSERT_TRUE(destroyed);
  ASSERT_TRUE(on_closed);
}

TEST(AsyncDestroyable, MakeShared) {
  TestAsyncDestroyable([](Future<> gate, bool* destroyed) {
    return MakeSharedAsync<GatingDestroyable>(gate, destroyed);
  });
}

TEST(AsyncDestroyable, MakeUnique) {
  TestAsyncDestroyable([](Future<> gate, bool* destroyed) {
    return MakeUniqueAsync<GatingDestroyable>(gate, destroyed);
  });
}

TEST(AsyncTaskGroup, Basic) {
  AsyncTaskGroup task_group;
  Future<> fut1 = Future<>::Make();
  Future<> fut2 = Future<>::Make();
  ASSERT_OK(task_group.AddTask(fut1));
  ASSERT_OK(task_group.AddTask(fut2));
  Future<> all_done = task_group.WaitForTasksToFinish();
  AssertNotFinished(all_done);
  fut1.MarkFinished();
  AssertNotFinished(all_done);
  fut2.MarkFinished();
  ASSERT_FINISHES_OK(all_done);
}

TEST(AsyncTaskGroup, NoTasks) {
  AsyncTaskGroup task_group;
  ASSERT_FINISHES_OK(task_group.WaitForTasksToFinish());
}

TEST(AsyncTaskGroup, AddAfterDone) {
  AsyncTaskGroup task_group;
  ASSERT_FINISHES_OK(task_group.WaitForTasksToFinish());
  ASSERT_RAISES(Invalid, task_group.AddTask(Future<>::Make()));
}

TEST(AsyncTaskGroup, AddAfterWaitButBeforeFinish) {
  AsyncTaskGroup task_group;
  Future<> task_one = Future<>::Make();
  ASSERT_OK(task_group.AddTask(task_one));
  Future<> finish_fut = task_group.WaitForTasksToFinish();
  AssertNotFinished(finish_fut);
  Future<> task_two = Future<>::Make();
  ASSERT_OK(task_group.AddTask(task_two));
  AssertNotFinished(finish_fut);
  task_one.MarkFinished();
  AssertNotFinished(finish_fut);
  task_two.MarkFinished();
  AssertFinished(finish_fut);
  ASSERT_FINISHES_OK(finish_fut);
}

TEST(AsyncTaskGroup, Error) {
  AsyncTaskGroup task_group;
  Future<> failed_task = Future<>::MakeFinished(Status::Invalid("XYZ"));
  ASSERT_OK(task_group.AddTask(failed_task));
  ASSERT_FINISHES_AND_RAISES(Invalid, task_group.WaitForTasksToFinish());
}

TEST(AsyncTaskGroup, TaskFinishesAfterError) {
  AsyncTaskGroup task_group;
  Future<> fut1 = Future<>::Make();
  ASSERT_OK(task_group.AddTask(fut1));
  ASSERT_OK(task_group.AddTask(Future<>::MakeFinished(Status::Invalid("XYZ"))));
  Future<> finished_fut = task_group.WaitForTasksToFinish();
  AssertNotFinished(finished_fut);
  fut1.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(Invalid, finished_fut);
}

TEST(AsyncTaskGroup, AddAfterFailed) {
  AsyncTaskGroup task_group;
  ASSERT_OK(task_group.AddTask(Future<>::MakeFinished(Status::Invalid("XYZ"))));
  ASSERT_RAISES(Invalid, task_group.AddTask(Future<>::Make()));
  ASSERT_FINISHES_AND_RAISES(Invalid, task_group.WaitForTasksToFinish());
}

TEST(AsyncTaskGroup, FailAfterAdd) {
  AsyncTaskGroup task_group;
  Future<> will_fail = Future<>::Make();
  ASSERT_OK(task_group.AddTask(will_fail));
  Future<> added_later_and_passes = Future<>::Make();
  ASSERT_OK(task_group.AddTask(added_later_and_passes));
  will_fail.MarkFinished(Status::Invalid("XYZ"));
  ASSERT_RAISES(Invalid, task_group.AddTask(Future<>::Make()));
  Future<> finished_fut = task_group.WaitForTasksToFinish();
  AssertNotFinished(finished_fut);
  added_later_and_passes.MarkFinished();
  AssertFinished(finished_fut);
  ASSERT_FINISHES_AND_RAISES(Invalid, finished_fut);
}

}  // namespace util
}  // namespace arrow
