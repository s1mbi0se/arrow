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

#include "gandiva/to_date_holder.h"

#include <algorithm>
#include <string>

#include "arrow/util/value_parsing.h"
#include "arrow/vendored/datetime.h"
#include "gandiva/node.h"

namespace gandiva {

Status ToDateHolder::Make(const FunctionNode& node,
                          std::shared_ptr<ToDateHolder>* holder) {
  std::string function_name("to_date");
  return gandiva::ToDateFunctionsHolder<ToDateHolder>::Make(node, holder, function_name);
}

Status ToDateHolder::Make(const std::string& sql_pattern, int32_t suppress_errors,
                          std::shared_ptr<ToDateHolder>* holder) {
  return gandiva::ToDateFunctionsHolder<ToDateHolder>::Make(sql_pattern, suppress_errors, holder);
}
}  // namespace gandiva
