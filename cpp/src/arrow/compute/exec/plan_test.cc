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

#include <functional>
#include <memory>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/vector.h"

using testing::ElementsAre;
using testing::ElementsAreArray;
using testing::HasSubstr;
using testing::Optional;
using testing::UnorderedElementsAreArray;

namespace arrow {

namespace compute {

TEST(ExecPlanConstruction, Empty) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());

  ASSERT_THAT(plan->Validate(), Raises(StatusCode::Invalid));
}

TEST(ExecPlanConstruction, SingleNode) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  auto node = MakeDummyNode(plan.get(), "dummy", /*inputs=*/{}, /*num_outputs=*/0);
  ASSERT_OK(plan->Validate());
  ASSERT_THAT(plan->sources(), ElementsAre(node));
  ASSERT_THAT(plan->sinks(), ElementsAre(node));

  ASSERT_OK_AND_ASSIGN(plan, ExecPlan::Make());
  node = MakeDummyNode(plan.get(), "dummy", /*inputs=*/{}, /*num_outputs=*/1);
  // Output not bound
  ASSERT_THAT(plan->Validate(), Raises(StatusCode::Invalid));
}

TEST(ExecPlanConstruction, SourceSink) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  auto source = MakeDummyNode(plan.get(), "source", /*inputs=*/{}, /*num_outputs=*/1);
  auto sink = MakeDummyNode(plan.get(), "sink", /*inputs=*/{source}, /*num_outputs=*/0);

  ASSERT_OK(plan->Validate());
  EXPECT_THAT(plan->sources(), ElementsAre(source));
  EXPECT_THAT(plan->sinks(), ElementsAre(sink));
}

TEST(ExecPlanConstruction, MultipleNode) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());

  auto source1 = MakeDummyNode(plan.get(), "source1", /*inputs=*/{}, /*num_outputs=*/2);

  auto source2 = MakeDummyNode(plan.get(), "source2", /*inputs=*/{}, /*num_outputs=*/1);

  auto process1 =
      MakeDummyNode(plan.get(), "process1", /*inputs=*/{source1}, /*num_outputs=*/2);

  auto process2 = MakeDummyNode(plan.get(), "process1", /*inputs=*/{source1, source2},
                                /*num_outputs=*/1);

  auto process3 =
      MakeDummyNode(plan.get(), "process3", /*inputs=*/{process1, process2, process1},
                    /*num_outputs=*/1);

  auto sink = MakeDummyNode(plan.get(), "sink", /*inputs=*/{process3}, /*num_outputs=*/0);

  ASSERT_OK(plan->Validate());
  ASSERT_THAT(plan->sources(), ElementsAre(source1, source2));
  ASSERT_THAT(plan->sinks(), ElementsAre(sink));
}

struct StartStopTracker {
  std::vector<std::string> started, stopped;

  StartProducingFunc start_producing_func(Status st = Status::OK()) {
    return [this, st](ExecNode* node) {
      started.push_back(node->label());
      return st;
    };
  }

  StopProducingFunc stop_producing_func() {
    return [this](ExecNode* node) { stopped.push_back(node->label()); };
  }
};

TEST(ExecPlan, DummyStartProducing) {
  StartStopTracker t;

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());

  auto source1 = MakeDummyNode(plan.get(), "source1", /*inputs=*/{}, /*num_outputs=*/2,
                               t.start_producing_func(), t.stop_producing_func());

  auto source2 = MakeDummyNode(plan.get(), "source2", /*inputs=*/{}, /*num_outputs=*/1,
                               t.start_producing_func(), t.stop_producing_func());

  auto process1 =
      MakeDummyNode(plan.get(), "process1", /*inputs=*/{source1}, /*num_outputs=*/2,
                    t.start_producing_func(), t.stop_producing_func());

  auto process2 =
      MakeDummyNode(plan.get(), "process2", /*inputs=*/{process1, source2},
                    /*num_outputs=*/1, t.start_producing_func(), t.stop_producing_func());

  auto process3 =
      MakeDummyNode(plan.get(), "process3", /*inputs=*/{process1, source1, process2},
                    /*num_outputs=*/1, t.start_producing_func(), t.stop_producing_func());

  MakeDummyNode(plan.get(), "sink", /*inputs=*/{process3}, /*num_outputs=*/0,
                t.start_producing_func(), t.stop_producing_func());

  ASSERT_OK(plan->Validate());
  ASSERT_EQ(t.started.size(), 0);
  ASSERT_EQ(t.stopped.size(), 0);

  ASSERT_OK(plan->StartProducing());
  // Note that any correct reverse topological order may do
  ASSERT_THAT(t.started, ElementsAre("sink", "process3", "process2", "process1",
                                     "source2", "source1"));

  plan->StopProducing();
  ASSERT_THAT(plan->finished(), Finishes(Ok()));
  // Note that any correct topological order may do
  ASSERT_THAT(t.stopped, ElementsAre("source1", "source2", "process1", "process2",
                                     "process3", "sink"));

  ASSERT_THAT(plan->StartProducing(),
              Raises(StatusCode::Invalid, HasSubstr("restarted")));
}

TEST(ExecPlan, DummyStartProducingError) {
  StartStopTracker t;

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  auto source1 = MakeDummyNode(
      plan.get(), "source1", /*num_inputs=*/{}, /*num_outputs=*/2,
      t.start_producing_func(Status::NotImplemented("zzz")), t.stop_producing_func());

  auto source2 =
      MakeDummyNode(plan.get(), "source2", /*num_inputs=*/{}, /*num_outputs=*/1,
                    t.start_producing_func(), t.stop_producing_func());

  auto process1 = MakeDummyNode(
      plan.get(), "process1", /*num_inputs=*/{source1}, /*num_outputs=*/2,
      t.start_producing_func(Status::IOError("xxx")), t.stop_producing_func());

  auto process2 =
      MakeDummyNode(plan.get(), "process2", /*num_inputs=*/{process1, source2},
                    /*num_outputs=*/1, t.start_producing_func(), t.stop_producing_func());

  auto process3 =
      MakeDummyNode(plan.get(), "process3", /*num_inputs=*/{process1, source1, process2},
                    /*num_outputs=*/1, t.start_producing_func(), t.stop_producing_func());

  MakeDummyNode(plan.get(), "sink", /*num_inputs=*/{process3}, /*num_outputs=*/0,
                t.start_producing_func(), t.stop_producing_func());

  ASSERT_OK(plan->Validate());
  ASSERT_EQ(t.started.size(), 0);
  ASSERT_EQ(t.stopped.size(), 0);

  // `process1` raises IOError
  ASSERT_THAT(plan->StartProducing(), Raises(StatusCode::IOError));
  ASSERT_THAT(t.started, ElementsAre("sink", "process3", "process2", "process1"));
  // Nodes that started successfully were stopped in reverse order
  ASSERT_THAT(t.stopped, ElementsAre("process2", "process3", "sink"));
}

TEST(ExecPlanExecution, SourceSink) {
  for (bool slow : {false, true}) {
    SCOPED_TRACE(slow ? "slowed" : "unslowed");

    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel ? "parallel" : "single threaded");

      ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
      AsyncGenerator<util::optional<ExecBatch>> sink_gen;

      auto basic_data = MakeBasicBatches();

      ASSERT_OK(Declaration::Sequence(
                    {
                        {"source", SourceNodeOptions{basic_data.schema,
                                                     basic_data.gen(parallel, slow)}},
                        {"sink", SinkNodeOptions{&sink_gen}},
                    })
                    .AddToPlan(plan.get()));

      ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
                  Finishes(ResultWith(UnorderedElementsAreArray(basic_data.batches))));
    }
  }
}

TEST(ExecPlanExecution, SourceOrderBy) {
  std::vector<ExecBatch> expected = {
      ExecBatchFromJSON({int32(), boolean()},
                        "[[4, false], [5, null], [6, false], [7, false], [null, true]]")};
  for (bool slow : {false, true}) {
    SCOPED_TRACE(slow ? "slowed" : "unslowed");

    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel ? "parallel" : "single threaded");

      ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
      AsyncGenerator<util::optional<ExecBatch>> sink_gen;

      auto basic_data = MakeBasicBatches();

      SortOptions options({SortKey("i32", SortOrder::Ascending)});
      ASSERT_OK(Declaration::Sequence(
                    {
                        {"source", SourceNodeOptions{basic_data.schema,
                                                     basic_data.gen(parallel, slow)}},
                        {"order_by_sink", OrderBySinkNodeOptions{options, &sink_gen}},
                    })
                    .AddToPlan(plan.get()));

      ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
                  Finishes(ResultWith(ElementsAreArray(expected))));
    }
  }
}

TEST(ExecPlanExecution, SourceSinkError) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  AsyncGenerator<util::optional<ExecBatch>> sink_gen;

  auto basic_data = MakeBasicBatches();
  auto it = basic_data.batches.begin();
  AsyncGenerator<util::optional<ExecBatch>> error_source_gen =
      [&]() -> Result<util::optional<ExecBatch>> {
    if (it == basic_data.batches.end()) {
      return Status::Invalid("Artificial error");
    }
    return util::make_optional(*it++);
  };

  ASSERT_OK(Declaration::Sequence(
                {
                    {"source", SourceNodeOptions{basic_data.schema, error_source_gen}},
                    {"sink", SinkNodeOptions{&sink_gen}},
                })
                .AddToPlan(plan.get()));

  ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
              Finishes(Raises(StatusCode::Invalid, HasSubstr("Artificial"))));
}

TEST(ExecPlanExecution, StressSourceSink) {
  for (bool slow : {false, true}) {
    SCOPED_TRACE(slow ? "slowed" : "unslowed");

    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel ? "parallel" : "single threaded");

      int num_batches = slow && !parallel ? 30 : 300;

      ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
      AsyncGenerator<util::optional<ExecBatch>> sink_gen;

      auto random_data = MakeRandomBatches(
          schema({field("a", int32()), field("b", boolean())}), num_batches);

      ASSERT_OK(Declaration::Sequence(
                    {
                        {"source", SourceNodeOptions{random_data.schema,
                                                     random_data.gen(parallel, slow)}},
                        {"sink", SinkNodeOptions{&sink_gen}},
                    })
                    .AddToPlan(plan.get()));

      ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
                  Finishes(ResultWith(UnorderedElementsAreArray(random_data.batches))));
    }
  }
}

TEST(ExecPlanExecution, StressSourceOrderBy) {
  auto input_schema = schema({field("a", int32()), field("b", boolean())});
  for (bool slow : {false, true}) {
    SCOPED_TRACE(slow ? "slowed" : "unslowed");

    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel ? "parallel" : "single threaded");

      int num_batches = slow && !parallel ? 30 : 300;

      ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
      AsyncGenerator<util::optional<ExecBatch>> sink_gen;

      auto random_data = MakeRandomBatches(input_schema, num_batches);

      SortOptions options({SortKey("a", SortOrder::Ascending)});
      ASSERT_OK(Declaration::Sequence(
                    {
                        {"source", SourceNodeOptions{random_data.schema,
                                                     random_data.gen(parallel, slow)}},
                        {"order_by_sink", OrderBySinkNodeOptions{options, &sink_gen}},
                    })
                    .AddToPlan(plan.get()));

      // Check that data is sorted appropriately
      ASSERT_FINISHES_OK_AND_ASSIGN(auto exec_batches,
                                    StartAndCollect(plan.get(), sink_gen));
      ASSERT_OK_AND_ASSIGN(auto actual, TableFromExecBatches(input_schema, exec_batches));
      ASSERT_OK_AND_ASSIGN(auto original,
                           TableFromExecBatches(input_schema, random_data.batches));
      ASSERT_OK_AND_ASSIGN(auto sort_indices, SortIndices(original, options));
      ASSERT_OK_AND_ASSIGN(auto expected, Take(original, sort_indices));
      AssertTablesEqual(*actual, *expected.table());
    }
  }
}

TEST(ExecPlanExecution, StressSourceSinkStopped) {
  for (bool slow : {false, true}) {
    SCOPED_TRACE(slow ? "slowed" : "unslowed");

    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel ? "parallel" : "single threaded");

      int num_batches = slow && !parallel ? 30 : 300;

      ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
      AsyncGenerator<util::optional<ExecBatch>> sink_gen;

      auto random_data = MakeRandomBatches(
          schema({field("a", int32()), field("b", boolean())}), num_batches);

      ASSERT_OK(Declaration::Sequence(
                    {
                        {"source", SourceNodeOptions{random_data.schema,
                                                     random_data.gen(parallel, slow)}},
                        {"sink", SinkNodeOptions{&sink_gen}},
                    })
                    .AddToPlan(plan.get()));

      ASSERT_OK(plan->Validate());
      ASSERT_OK(plan->StartProducing());

      EXPECT_THAT(sink_gen(), Finishes(ResultWith(Optional(random_data.batches[0]))));

      plan->StopProducing();
      ASSERT_THAT(plan->finished(), Finishes(Ok()));
    }
  }
}

TEST(ExecPlanExecution, SourceFilterSink) {
  auto basic_data = MakeBasicBatches();

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  AsyncGenerator<util::optional<ExecBatch>> sink_gen;

  ASSERT_OK(Declaration::Sequence(
                {
                    {"source", SourceNodeOptions{basic_data.schema,
                                                 basic_data.gen(/*parallel=*/false,
                                                                /*slow=*/false)}},
                    {"filter", FilterNodeOptions{equal(field_ref("i32"), literal(6))}},
                    {"sink", SinkNodeOptions{&sink_gen}},
                })
                .AddToPlan(plan.get()));

  ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
              Finishes(ResultWith(UnorderedElementsAreArray(
                  {ExecBatchFromJSON({int32(), boolean()}, "[]"),
                   ExecBatchFromJSON({int32(), boolean()}, "[[6, false]]")}))));
}

TEST(ExecPlanExecution, SourceProjectSink) {
  auto basic_data = MakeBasicBatches();

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  AsyncGenerator<util::optional<ExecBatch>> sink_gen;

  ASSERT_OK(Declaration::Sequence(
                {
                    {"source", SourceNodeOptions{basic_data.schema,
                                                 basic_data.gen(/*parallel=*/false,
                                                                /*slow=*/false)}},
                    {"project",
                     ProjectNodeOptions{{
                                            not_(field_ref("bool")),
                                            call("add", {field_ref("i32"), literal(1)}),
                                        },
                                        {"!bool", "i32 + 1"}}},
                    {"sink", SinkNodeOptions{&sink_gen}},
                })
                .AddToPlan(plan.get()));

  ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
              Finishes(ResultWith(UnorderedElementsAreArray(
                  {ExecBatchFromJSON({boolean(), int32()}, "[[false, null], [true, 5]]"),
                   ExecBatchFromJSON({boolean(), int32()},
                                     "[[null, 6], [true, 7], [true, 8]]")}))));
}

namespace {

BatchesWithSchema MakeGroupableBatches(int multiplicity = 1) {
  BatchesWithSchema out;

  out.batches = {ExecBatchFromJSON({int32(), utf8()}, R"([
                   [12, "alfa"],
                   [7,  "beta"],
                   [3,  "alfa"]
                 ])"),
                 ExecBatchFromJSON({int32(), utf8()}, R"([
                   [-2, "alfa"],
                   [-1, "gama"],
                   [3,  "alfa"]
                 ])"),
                 ExecBatchFromJSON({int32(), utf8()}, R"([
                   [5,  "gama"],
                   [3,  "beta"],
                   [-8, "alfa"]
                 ])")};

  size_t batch_count = out.batches.size();
  for (int repeat = 1; repeat < multiplicity; ++repeat) {
    for (size_t i = 0; i < batch_count; ++i) {
      out.batches.push_back(out.batches[i]);
    }
  }

  out.schema = schema({field("i32", int32()), field("str", utf8())});

  return out;
}

}  // namespace

TEST(ExecPlanExecution, SourceGroupedSum) {
  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel ? "parallel/merged" : "serial");

    auto input = MakeGroupableBatches(/*multiplicity=*/parallel ? 100 : 1);

    ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
    AsyncGenerator<util::optional<ExecBatch>> sink_gen;

    ASSERT_OK(Declaration::Sequence(
                  {
                      {"source", SourceNodeOptions{input.schema,
                                                   input.gen(parallel, /*slow=*/false)}},
                      {"aggregate",
                       AggregateNodeOptions{/*aggregates=*/{{"hash_sum", nullptr}},
                                            /*targets=*/{"i32"}, /*names=*/{"sum(i32)"},
                                            /*keys=*/{"str"}}},
                      {"sink", SinkNodeOptions{&sink_gen}},
                  })
                  .AddToPlan(plan.get()));

    ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
                Finishes(ResultWith(UnorderedElementsAreArray({ExecBatchFromJSON(
                    {int64(), utf8()},
                    parallel ? R"([[800, "alfa"], [1000, "beta"], [400, "gama"]])"
                             : R"([[8, "alfa"], [10, "beta"], [4, "gama"]])")}))));
  }
}

TEST(ExecPlanExecution, SourceFilterProjectGroupedSumFilter) {
  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel ? "parallel/merged" : "serial");

    int batch_multiplicity = parallel ? 100 : 1;
    auto input = MakeGroupableBatches(/*multiplicity=*/batch_multiplicity);

    ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
    AsyncGenerator<util::optional<ExecBatch>> sink_gen;

    ASSERT_OK(
        Declaration::Sequence(
            {
                {"source",
                 SourceNodeOptions{input.schema, input.gen(parallel, /*slow=*/false)}},
                {"filter",
                 FilterNodeOptions{greater_equal(field_ref("i32"), literal(0))}},
                {"project", ProjectNodeOptions{{
                                field_ref("str"),
                                call("multiply", {field_ref("i32"), literal(2)}),
                            }}},
                {"aggregate", AggregateNodeOptions{/*aggregates=*/{{"hash_sum", nullptr}},
                                                   /*targets=*/{"multiply(i32, 2)"},
                                                   /*names=*/{"sum(multiply(i32, 2))"},
                                                   /*keys=*/{"str"}}},
                {"filter", FilterNodeOptions{greater(field_ref("sum(multiply(i32, 2))"),
                                                     literal(10 * batch_multiplicity))}},
                {"sink", SinkNodeOptions{&sink_gen}},
            })
            .AddToPlan(plan.get()));

    ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
                Finishes(ResultWith(UnorderedElementsAreArray({ExecBatchFromJSON(
                    {int64(), utf8()}, parallel ? R"([[3600, "alfa"], [2000, "beta"]])"
                                                : R"([[36, "alfa"], [20, "beta"]])")}))));
  }
}

TEST(ExecPlanExecution, SourceFilterProjectGroupedSumOrderBy) {
  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel ? "parallel/merged" : "serial");

    int batch_multiplicity = parallel ? 100 : 1;
    auto input = MakeGroupableBatches(/*multiplicity=*/batch_multiplicity);

    ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
    AsyncGenerator<util::optional<ExecBatch>> sink_gen;

    SortOptions options({SortKey("str", SortOrder::Descending)});
    ASSERT_OK(
        Declaration::Sequence(
            {
                {"source",
                 SourceNodeOptions{input.schema, input.gen(parallel, /*slow=*/false)}},
                {"filter",
                 FilterNodeOptions{greater_equal(field_ref("i32"), literal(0))}},
                {"project", ProjectNodeOptions{{
                                field_ref("str"),
                                call("multiply", {field_ref("i32"), literal(2)}),
                            }}},
                {"aggregate", AggregateNodeOptions{/*aggregates=*/{{"hash_sum", nullptr}},
                                                   /*targets=*/{"multiply(i32, 2)"},
                                                   /*names=*/{"sum(multiply(i32, 2))"},
                                                   /*keys=*/{"str"}}},
                {"filter", FilterNodeOptions{greater(field_ref("sum(multiply(i32, 2))"),
                                                     literal(10 * batch_multiplicity))}},
                {"order_by_sink", OrderBySinkNodeOptions{options, &sink_gen}},
            })
            .AddToPlan(plan.get()));

    ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
                Finishes(ResultWith(ElementsAreArray({ExecBatchFromJSON(
                    {int64(), utf8()}, parallel ? R"([[2000, "beta"], [3600, "alfa"]])"
                                                : R"([[20, "beta"], [36, "alfa"]])")}))));
  }
}

TEST(ExecPlanExecution, SourceScalarAggSink) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  AsyncGenerator<util::optional<ExecBatch>> sink_gen;

  auto basic_data = MakeBasicBatches();

  ASSERT_OK(Declaration::Sequence(
                {
                    {"source", SourceNodeOptions{basic_data.schema,
                                                 basic_data.gen(/*parallel=*/false,
                                                                /*slow=*/false)}},
                    {"aggregate", AggregateNodeOptions{
                                      /*aggregates=*/{{"sum", nullptr}, {"any", nullptr}},
                                      /*targets=*/{"i32", "bool"},
                                      /*names=*/{"sum(i32)", "any(bool)"}}},
                    {"sink", SinkNodeOptions{&sink_gen}},
                })
                .AddToPlan(plan.get()));

  ASSERT_THAT(
      StartAndCollect(plan.get(), sink_gen),
      Finishes(ResultWith(UnorderedElementsAreArray({
          ExecBatchFromJSON({ValueDescr::Scalar(int64()), ValueDescr::Scalar(boolean())},
                            "[[22, true]]"),
      }))));
}

TEST(ExecPlanExecution, AggregationPreservesOptions) {
  // ARROW-13638: aggregation nodes initialize per-thread kernel state lazily
  // and need to keep a copy/strong reference to function options
  {
    ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
    AsyncGenerator<util::optional<ExecBatch>> sink_gen;

    auto basic_data = MakeBasicBatches();

    {
      auto options = std::make_shared<TDigestOptions>(TDigestOptions::Defaults());
      ASSERT_OK(Declaration::Sequence(
                    {
                        {"source", SourceNodeOptions{basic_data.schema,
                                                     basic_data.gen(/*parallel=*/false,
                                                                    /*slow=*/false)}},
                        {"aggregate",
                         AggregateNodeOptions{/*aggregates=*/{{"tdigest", options.get()}},
                                              /*targets=*/{"i32"},
                                              /*names=*/{"tdigest(i32)"}}},
                        {"sink", SinkNodeOptions{&sink_gen}},
                    })
                    .AddToPlan(plan.get()));
    }

    ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
                Finishes(ResultWith(UnorderedElementsAreArray({
                    ExecBatchFromJSON({ValueDescr::Array(float64())}, "[[5.5]]"),
                }))));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
    AsyncGenerator<util::optional<ExecBatch>> sink_gen;

    auto data = MakeGroupableBatches(/*multiplicity=*/100);

    {
      auto options = std::make_shared<CountOptions>(CountOptions::Defaults());
      ASSERT_OK(
          Declaration::Sequence(
              {
                  {"source", SourceNodeOptions{data.schema, data.gen(/*parallel=*/false,
                                                                     /*slow=*/false)}},
                  {"aggregate",
                   AggregateNodeOptions{/*aggregates=*/{{"hash_count", options.get()}},
                                        /*targets=*/{"i32"},
                                        /*names=*/{"count(i32)"},
                                        /*keys=*/{"str"}}},
                  {"sink", SinkNodeOptions{&sink_gen}},
              })
              .AddToPlan(plan.get()));
    }

    ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
                Finishes(ResultWith(UnorderedElementsAreArray({
                    ExecBatchFromJSON({int64(), utf8()},
                                      R"([[500, "alfa"], [200, "beta"], [200, "gama"]])"),
                }))));
  }
}

TEST(ExecPlanExecution, ScalarSourceScalarAggSink) {
  // ARROW-9056: scalar aggregation can be done over scalars, taking
  // into account batch.length > 1 (e.g. a partition column)
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
  AsyncGenerator<util::optional<ExecBatch>> sink_gen;

  BatchesWithSchema scalar_data;
  scalar_data.batches = {
      ExecBatchFromJSON({ValueDescr::Scalar(int32()), ValueDescr::Scalar(boolean())},
                        "[[5, false], [5, false], [5, false]]"),
      ExecBatchFromJSON({int32(), boolean()}, "[[5, true], [6, false], [7, true]]")};
  scalar_data.schema = schema({field("a", int32()), field("b", boolean())});

  // index can't be tested as it's order-dependent
  // mode/quantile can't be tested as they're technically vector kernels
  ASSERT_OK(
      Declaration::Sequence(
          {
              {"source",
               SourceNodeOptions{scalar_data.schema, scalar_data.gen(/*parallel=*/false,
                                                                     /*slow=*/false)}},
              {"aggregate", AggregateNodeOptions{
                                /*aggregates=*/{{"all", nullptr},
                                                {"any", nullptr},
                                                {"count", nullptr},
                                                {"mean", nullptr},
                                                {"product", nullptr},
                                                {"stddev", nullptr},
                                                {"sum", nullptr},
                                                {"tdigest", nullptr},
                                                {"variance", nullptr}},
                                /*targets=*/{"b", "b", "a", "a", "a", "a", "a", "a", "a"},
                                /*names=*/
                                {"all(b)", "any(b)", "count(a)", "mean(a)", "product(a)",
                                 "stddev(a)", "sum(a)", "tdigest(a)", "variance(a)"}}},
              {"sink", SinkNodeOptions{&sink_gen}},
          })
          .AddToPlan(plan.get()));

  ASSERT_THAT(
      StartAndCollect(plan.get(), sink_gen),
      Finishes(ResultWith(UnorderedElementsAreArray({
          ExecBatchFromJSON(
              {ValueDescr::Scalar(boolean()), ValueDescr::Scalar(boolean()),
               ValueDescr::Scalar(int64()), ValueDescr::Scalar(float64()),
               ValueDescr::Scalar(int64()), ValueDescr::Scalar(float64()),
               ValueDescr::Scalar(int64()), ValueDescr::Array(float64()),
               ValueDescr::Scalar(float64())},
              R"([[false, true, 6, 5.5, 26250, 0.7637626158259734, 33, 5.0, 0.5833333333333334]])"),
      }))));
}

}  // namespace compute
}  // namespace arrow
