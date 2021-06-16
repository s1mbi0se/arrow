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

#include <chrono>
#include "gandiva/projector.h"
#include "gandiva/filter.h"

#include <gtest/gtest.h>

#include <cmath>

#include <unistd.h>

#include "arrow/memory_pool.h"
#include "gandiva/literal_holder.h"
#include "gandiva/node.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"


namespace gandiva {

bool suite_test = true; // set to false to run the each test separately without error

class TestObjectCache : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = arrow::default_memory_pool();
    setenv("GANDIVA_CACHE_SIZE", "5120", 1);
    setenv("GANDIVA_DISK_CAPACITY_SIZE", "10240", 1);
    setenv("GANDIVA_DISK_RESERVED_SIZE", "20480", 1);
    // Setup arrow log severity threshold to debug level.
    arrow::util::ArrowLog::StartArrowLog("", arrow::util::ArrowLogLevel::ARROW_DEBUG);
  }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestObjectCache, TestProjectorObjectCache) {
  // schema for input fields
  auto field0 = field("f0", arrow::int32());
  auto field1 = field("f2", arrow::int32());
  auto field2 = field("f0", arrow::float64());
  auto field3 = field("f2", arrow::float64());
  auto field4 = field("f0", arrow::utf8());

  auto schema1 = arrow::schema({field0, field1});
  auto schema2 = arrow::schema({field2, field3});
  auto schema3 = arrow::schema({field4});

  // output fields
  auto field_sum = field("add", arrow::int32());
  auto field_sub = field("subtract", arrow::int32());
  auto field_mul = field("multiply", arrow::int32());
  auto field_div = field("divide", arrow::int32());
  auto field_eq = field("equal", arrow::boolean());
  auto field_lt = field("less_than", arrow::boolean());
  auto field_logb = arrow::field("logb", arrow::float64());
  auto field_power = arrow::field("power", arrow::float64());
  auto res_float8 = field("res_float8", arrow::float64());
  auto res_int4 = field("castBIGINT", arrow::int32());

  // Build expressions for schema1
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto sub_expr =
      TreeExprBuilder::MakeExpression("subtract", {field0, field1}, field_sub);
  auto mul_expr =
      TreeExprBuilder::MakeExpression("multiply", {field0, field1}, field_mul);
  auto div_expr = TreeExprBuilder::MakeExpression("divide", {field0, field1}, field_div);
  auto eq_expr = TreeExprBuilder::MakeExpression("equal", {field0, field1}, field_eq);
  auto lt_expr = TreeExprBuilder::MakeExpression("less_than", {field0, field1}, field_lt);

  // Build expressions for schema2
  auto logb_expr = TreeExprBuilder::MakeExpression("log", {field2, field3}, field_logb);
  auto power_expr =
      TreeExprBuilder::MakeExpression("power", {field2, field3}, field_power);

  // Build expressions for schema3
  auto cast_expr_float8 =
      TreeExprBuilder::MakeExpression("castFLOAT8", {field4}, res_float8);
  auto cast_expr_int8 = TreeExprBuilder::MakeExpression("castINT", {field4}, res_int4);

  auto configuration = TestConfiguration();

  // Uses field0, field1 and schema1
  ExpressionVector exprVec1 = {sum_expr};
  ExpressionVector exprVec2 = {sub_expr};
  ExpressionVector exprVec3 = {mul_expr};
  ExpressionVector exprVec4 = {div_expr};
  ExpressionVector exprVec5 = {eq_expr};
  ExpressionVector exprVec6 = {lt_expr};

  // Uses field2, field3 and schema2
  ExpressionVector exprVec7 = {logb_expr};
  ExpressionVector exprVec8 = {power_expr};

  // Uses field4 and schema3
  ExpressionVector exprVec9 = {cast_expr_float8};
  ExpressionVector exprVec10 = {cast_expr_int8};

  unsigned int microsecond = 1000000;
  //usleep(0.5 * microsecond);//sleeps for 0.5 second, only for heap tracking

  // 1st projector
  std::shared_ptr<Projector> projector1;
  auto status = Projector::Make(schema1, exprVec1, configuration, &projector1);
  ASSERT_OK(status);
  ASSERT_EQ(1352, projector1->GetUsedCacheSize());
  //usleep(0.5 * microsecond);//sleeps for 0.5 second, only for heap tracking

  // 2nd projector
  std::shared_ptr<Projector> projector2;
  status = Projector::Make(schema1, exprVec2, configuration, &projector2);
  ASSERT_OK(status);
  ASSERT_EQ(2704, projector2->GetUsedCacheSize());
  // usleep(0.5 * microsecond);//sleeps for 0.5 second, only for heap tracking

  // 3rd projector
  std::shared_ptr<Projector> projector3;
  status = Projector::Make(schema1, exprVec3, configuration, &projector3);
  ASSERT_OK(status);
  ASSERT_EQ(4056, projector3->GetUsedCacheSize());


  projector1.reset();
  //usleep(0.5 * microsecond);//sleeps for 0.5 second, only for heap tracking
  projector2.reset();
  //usleep(0.5 * microsecond);//sleeps for 0.5 second, only for heap tracking
  projector3.reset();
  //usleep(0.5 * microsecond);//sleeps for 0.5 second, only for heap tracking

  // Dummy projector, just to access the cache that the projectors used
  // to check if everything is still cached.
  std::shared_ptr<Projector> dummy_projector;
  ASSERT_EQ(4056, dummy_projector->GetUsedCacheSize());
}

TEST_F(TestObjectCache, TestProjectorObjectCacheEvict) {
  // schema for input fields
  auto field0 = field("f0", arrow::int32());
  auto field1 = field("f2", arrow::int32());
  auto field2 = field("f0", arrow::float64());
  auto field3 = field("f2", arrow::float64());
  auto field4 = field("f0", arrow::utf8());

  auto schema1 = arrow::schema({field0, field1});
  auto schema2 = arrow::schema({field2, field3});
  auto schema3 = arrow::schema({field4});

  // output fields
  auto field_sum = field("add", arrow::int32());
  auto field_sub = field("subtract", arrow::int32());
  auto field_mul = field("multiply", arrow::int32());
  auto field_div = field("divide", arrow::int32());
  auto field_eq = field("equal", arrow::boolean());
  auto field_lt = field("less_than", arrow::boolean());
  auto field_logb = arrow::field("logb", arrow::float64());
  auto field_power = arrow::field("power", arrow::float64());
  auto res_float8 = field("res_float8", arrow::float64());
  auto res_int4 = field("castBIGINT", arrow::int32());

  // Build expressions for schema1
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto sub_expr =
      TreeExprBuilder::MakeExpression("subtract", {field0, field1}, field_sub);
  auto mul_expr =
      TreeExprBuilder::MakeExpression("multiply", {field0, field1}, field_mul);
  auto div_expr = TreeExprBuilder::MakeExpression("divide", {field0, field1}, field_div);
  auto eq_expr = TreeExprBuilder::MakeExpression("equal", {field0, field1}, field_eq);
  auto lt_expr = TreeExprBuilder::MakeExpression("less_than", {field0, field1}, field_lt);

  // Build expressions for schema2
  auto logb_expr = TreeExprBuilder::MakeExpression("log", {field2, field3}, field_logb);
  auto power_expr =
      TreeExprBuilder::MakeExpression("power", {field2, field3}, field_power);

  // Build expressions for schema3
  auto cast_expr_float8 =
      TreeExprBuilder::MakeExpression("castFLOAT8", {field4}, res_float8);
  auto cast_expr_int8 = TreeExprBuilder::MakeExpression("castINT", {field4}, res_int4);

  auto configuration = TestConfiguration();

  // Uses field0, field1 and schema1
  ExpressionVector exprVec1 = {sum_expr};
  ExpressionVector exprVec2 = {sub_expr};
  ExpressionVector exprVec3 = {mul_expr};
  ExpressionVector exprVec4 = {div_expr};
  ExpressionVector exprVec5 = {eq_expr};
  ExpressionVector exprVec6 = {lt_expr};

  // Uses field2, field3 and schema2
  ExpressionVector exprVec7 = {logb_expr};
  ExpressionVector exprVec8 = {power_expr};

  // Uses field4 and schema3
  ExpressionVector exprVec9 = {cast_expr_float8};
  ExpressionVector exprVec10 = {cast_expr_int8};

  unsigned int microsecond = 1000000;
  //usleep(0.5 * microsecond);//sleeps for 0.5 second, only for heap tracking

  // 1st projector
  std::shared_ptr<Projector> projector1;
  auto status = Projector::Make(schema1, exprVec1, configuration, &projector1);
  ASSERT_OK(status);

  if (suite_test) {
    ASSERT_EQ(4056, projector1->GetUsedCacheSize());
  } else {
    ASSERT_EQ(1352, projector1->GetUsedCacheSize());
  }

  //usleep(0.5 * microsecond);//sleeps for 0.5 second, only for heap tracking

  // 2nd projector
  std::shared_ptr<Projector> projector2;
  status = Projector::Make(schema1, exprVec2, configuration, &projector2);
  ASSERT_OK(status);

  if (suite_test) {
    ASSERT_EQ(4056, projector2->GetUsedCacheSize());
  } else {
    ASSERT_EQ(2704, projector2->GetUsedCacheSize());
  }

  // usleep(0.5 * microsecond);//sleeps for 0.5 second, only for heap tracking

  // 3rd projector
  std::shared_ptr<Projector> projector3;
  status = Projector::Make(schema1, exprVec3, configuration, &projector3);
  ASSERT_OK(status);
  ASSERT_EQ(4056, projector3->GetUsedCacheSize());

  // 4th projector
  // Should delete the 1th projector from the cache, as the maximum capacity will be reached
  // if the fourth projector would be added to the cache.
  std::shared_ptr<Projector> projector4;
  status = Projector::Make(schema1, exprVec4, configuration, &projector3);
  ASSERT_OK(status);
  ASSERT_EQ(4896, projector4->GetUsedCacheSize());


  projector1.reset();
  //usleep(0.5 * microsecond);//sleeps for 0.5 second, only for heap tracking
  projector2.reset();
  //usleep(0.5 * microsecond);//sleeps for 0.5 second, only for heap tracking
  projector3.reset();
  //usleep(0.5 * microsecond);//sleeps for 0.5 second, only for heap tracking
  projector4.reset();
  //usleep(0.5 * microsecond);//sleeps for 0.5 second, only for heap tracking

  // Dummy projector, just to access the cache that the projectors used
  // to check if everything is still cached.
  std::shared_ptr<Projector> dummy_projector;
  ASSERT_EQ(4896, dummy_projector->GetUsedCacheSize());
}

TEST_F(TestObjectCache, TestFilterObjectCache) {
  // schema for input fields
  auto field0 = field("f0", arrow::int32());
  auto field1 = field("f1", arrow::int32());
  auto schema = arrow::schema({field0, field1});

  // Build condition f0 + f1 < 10 and f0 + f1 > 5;
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto sum_func =
      TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, arrow::int32());
  auto literal_10 = TreeExprBuilder::MakeLiteral((int32_t)10);
  auto literal_5 = TreeExprBuilder::MakeLiteral((int32_t)5);
  auto less_than_10 = TreeExprBuilder::MakeFunction("less_than", {sum_func, literal_10},
                                                    arrow::boolean());
  auto greater_than_5 = TreeExprBuilder::MakeFunction("greater_than", {sum_func, literal_5},
                                                      arrow::boolean());

  auto condition_less_than_10 = TreeExprBuilder::MakeCondition(less_than_10);
  auto condition_greater_than_5 = TreeExprBuilder::MakeCondition(greater_than_5);
  auto configuration = TestConfiguration();

  std::shared_ptr<Filter> filter1;
  auto status = Filter::Make(schema, condition_less_than_10, configuration, &filter1);
  ASSERT_EQ(4976, filter1->GetUsedCacheSize());
  ASSERT_FALSE(filter1->GetCompiledFromCache());
  ASSERT_OK(status);

  std::shared_ptr<Filter> filter2;
  status = Filter::Make(schema, condition_greater_than_5, configuration, &filter2);
  ASSERT_OK(status);
  ASSERT_FALSE(filter1->GetCompiledFromCache());
  ASSERT_EQ(5056, filter2->GetUsedCacheSize());

  filter1.reset();
  filter2.reset();

  std::shared_ptr<Filter> cached_filter1;
  status = Filter::Make(schema, condition_less_than_10, configuration, &cached_filter1);
  ASSERT_OK(status);
  ASSERT_TRUE(cached_filter1->GetCompiledFromCache());
  ASSERT_EQ(5056, cached_filter1->GetUsedCacheSize());

  cached_filter1.reset();

  std::shared_ptr<Filter> dummy_filter; // just to get the used cache by the filter.
  ASSERT_EQ(5056, dummy_filter->GetUsedCacheSize());
}

TEST_F(TestObjectCache, TestFilterObjectCacheEvict) {
  // schema for input fields
  auto field0 = field("f0", arrow::int32());
  auto field1 = field("f1", arrow::int32());
  auto schema = arrow::schema({field0, field1});

  // Build condition f0 + f1 < 10 and f0 + f1 > 5;
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);

  auto sum_func =
      TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, arrow::int32());
  auto sub_func =
      TreeExprBuilder::MakeFunction("subtract", {node_f0, node_f1}, arrow::int32());
  auto mul_func =
      TreeExprBuilder::MakeFunction("multiply", {node_f0, node_f1}, arrow::int32());
  auto div_func =
      TreeExprBuilder::MakeFunction("divide", {node_f0, node_f1}, arrow::int32());

  auto literal_10 = TreeExprBuilder::MakeLiteral((int32_t)10);
  auto literal_5 = TreeExprBuilder::MakeLiteral((int32_t)5);

  auto add_less_than_10 = TreeExprBuilder::MakeFunction("less_than", {sum_func, literal_10},
                                                        arrow::boolean());
  auto sub_greater_than_10 = TreeExprBuilder::MakeFunction("greater_than", {sub_func, literal_10},
                                                            arrow::boolean());

  auto mul_greater_than_5 = TreeExprBuilder::MakeFunction("greater_than", {mul_func, literal_5},
                                                      arrow::boolean());
  auto div_less_than_5 = TreeExprBuilder::MakeFunction("less_than", {div_func, literal_5},
                                                          arrow::boolean());

  auto condition_add_less_than_10= TreeExprBuilder::MakeCondition(add_less_than_10);
  auto condition_sub_greater_than_10 = TreeExprBuilder::MakeCondition(sub_greater_than_10);
  auto condition_mul_greater_than_5 = TreeExprBuilder::MakeCondition(mul_greater_than_5);
  auto condition_div_less_than_5 = TreeExprBuilder::MakeCondition(div_less_than_5);


  auto configuration = TestConfiguration();

  std::shared_ptr<Filter> filter1;
  auto status = Filter::Make(schema, condition_add_less_than_10, configuration, &filter1);
  ASSERT_OK(status);
  if(suite_test) {
    ASSERT_EQ(5056, filter1->GetUsedCacheSize());
    ASSERT_TRUE(filter1->GetCompiledFromCache());
  } else {
    ASSERT_EQ(1432, filter1->GetUsedCacheSize());
    ASSERT_FALSE(filter1->GetCompiledFromCache());
  }

  std::shared_ptr<Filter> filter2;
  status = Filter::Make(schema, condition_mul_greater_than_5, configuration, &filter2);
  ASSERT_OK(status);
  ASSERT_FALSE(filter2->GetCompiledFromCache());
  if(suite_test){
    ASSERT_EQ(4296, filter2->GetUsedCacheSize());
  } else {
    ASSERT_EQ(2864, filter2->GetUsedCacheSize());
  }

  std::shared_ptr<Filter> filter3;
  status = Filter::Make(schema, condition_sub_greater_than_10, configuration, &filter3);
  ASSERT_OK(status);
  ASSERT_FALSE(filter3->GetCompiledFromCache());
  ASSERT_EQ(4296, filter3->GetUsedCacheSize());

  std::shared_ptr<Filter> filter4;
  status = Filter::Make(schema, condition_div_less_than_5, configuration, &filter4);
  ASSERT_OK(status);
  ASSERT_FALSE(filter4->GetCompiledFromCache());
  ASSERT_EQ(3920, filter4->GetUsedCacheSize());

  filter1.reset();
  filter2.reset();
  filter3.reset();

  std::shared_ptr<Filter> dummy_filter; // just to get the used cache by the filter.
  ASSERT_EQ(3920, dummy_filter->GetUsedCacheSize());
}

TEST_F(TestObjectCache, TestProjectorObjectCacheForEachExpression) {
  auto start = std::chrono::high_resolution_clock::now();
  // schema for input fields
  auto field0 = field("f0", arrow::int32());
  auto field1 = field("f2", arrow::int32());


  auto schema1 = arrow::schema({field0, field1});


  // output fields
  auto field_sum = field("add", arrow::int32());
  auto field_sub = field("subtract", arrow::int32());
  auto field_mul = field("multiply", arrow::int32());
  auto field_div = field("divide", arrow::int32());
  auto field_eq = field("equal", arrow::boolean());
  auto field_lt = field("less_than", arrow::boolean());

  // Build expressions for schema1
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto sub_expr =
      TreeExprBuilder::MakeExpression("subtract", {field0, field1}, field_sub);
  auto mul_expr =
      TreeExprBuilder::MakeExpression("multiply", {field0, field1}, field_mul);
  auto div_expr = TreeExprBuilder::MakeExpression("divide", {field0, field1}, field_div);
  auto eq_expr = TreeExprBuilder::MakeExpression("equal", {field0, field1}, field_eq);
  auto lt_expr = TreeExprBuilder::MakeExpression("less_than", {field0, field1}, field_lt);


  auto configuration = TestConfiguration();

  // Uses field0, field1 and schema1
  ExpressionVector exprVec1 = {sum_expr, sub_expr, mul_expr};
  ExpressionVector exprVec2 = {sum_expr, sub_expr, mul_expr, div_expr, eq_expr, lt_expr};

  //unsigned int microsecond = 1000000;
  // usleep(0.5 * microsecond);//sleeps for 0.5 second, only for heap tracking


  std::shared_ptr<Projector> projector1;
  auto status = Projector::Make(schema1, exprVec1, configuration, &projector1);
  ASSERT_OK(status);

  projector1.reset();

  std::shared_ptr<Projector> projector2;
  status = Projector::Make(schema1, exprVec2, configuration, &projector2);
  ASSERT_OK(status);

  projector2.reset();
  auto stop = std::chrono::high_resolution_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

  // To get the value of duration use the count()
  // member function on the duration object
  std::cout <<"Elapsed time: " << duration.count() << std::endl;

  // Dummy projector, just to access the cache that the pr  ojectors used
  // to check if everything is still cached.
  std::shared_ptr<Projector> dummy_projector;
  //ASSERT_EQ(3600, dummy_projector->GetUsedCacheSize());
}

}
