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

#include "gandiva/interval_holder.h"

#include "gandiva/node.h"
#include "gandiva/regex_util.h"

namespace gandiva {

// pre-compiled pattern for matching period that only have numbers
static const RE2 period_only_contains_numbers(R"(\d+)");

// pre-compiled pattern for matching periods in 8601 formats that not contains weeks.
static const RE2 iso8601_complete_period(
    R"(P([[:digit:]]+Y)?([[:digit:]]+M)?([[:digit:]]+D)?)"
    R"(T([[:digit:]]+H)?([[:digit:]]+M)?([[:digit:]]+S|[[:digit:]]+\.[[:digit:]]+S)?)");

// pre-compiled pattern for matching periods in 8601 formats that not contain time
// (hours, minutes and seconds) information.
static const RE2 iso8601_period_without_time(
    R"(P([[:digit:]]+Y)?([[:digit:]]+M)?([[:digit:]]+D)?)");

// pre-compiled pattern for matching periods in 8601 formats that not contain time
// (hours, minutes and seconds) information.
static const std::regex period_not_contains_time(R"(^((?!T).)*$)");

// pre-compiled pattern for matching periods in 8601 formats that contains weeks inside
// them. The ISO8601 specification defines that if the string contains a week, it can not
// have other time granularities information, like day, years and months.
static const RE2 iso8601_period_with_weeks(R"(P([[:digit:]]+W){1})");

int64_t IntervalDaysHolder::GetIntervalDayFromMillis(const std::string& number_as_string,
                                                     bool* out_valid) {
  int64_t period_in_millis = std::stol(number_as_string);

  // It considers that a day has exactly 24 hours of duration
  int64_t days_to_standard_millis = 86400000;

  int64_t qty_days = period_in_millis / days_to_standard_millis;
  int64_t qty_millis = period_in_millis % days_to_standard_millis;

  // The response is a 64-bit integer where the lower half of the bytes represents the
  // number of the days and the other half represents the number of milliseconds.
  int64_t out = (qty_days & 0x00000000FFFFFFFF);
  out |= ((qty_millis << 32) & 0xFFFFFFFF00000000);

  *out_valid = true;
  return out;
}

int64_t IntervalDaysHolder::GetIntervalDayFromWeeks(const std::string& number_as_string,
                                                    bool* out_valid) {
  auto qty_weeks = static_cast<int64_t>(std::stod(number_as_string));

  // It considers that a day has exactly 24 hours of duration
  int64_t days_to_standard_millis = 86400000;
  int64_t week_to_qty_days = 7;

  int64_t millis_in_a_week = qty_weeks * week_to_qty_days * days_to_standard_millis;

  int64_t qty_days = millis_in_a_week / days_to_standard_millis;
  int64_t qty_millis = millis_in_a_week % days_to_standard_millis;

  // The response is a 64-bit integer where the lower half of the bytes represents the
  // number of the days and the other half represents the number of milliseconds.
  int64_t out = (qty_days & 0x00000000FFFFFFFF);
  out |= ((qty_millis << 32) & 0xFFFFFFFF00000000);

  *out_valid = true;
  return out;
}

int64_t IntervalDaysHolder::GetIntervalDayFromCompletePeriod(
    const std::string& days_in_period, const std::string& hours_in_period,
    const std::string& minutes_in_period, const std::string& seconds_in_period,
    bool* out_valid) {
  int64_t qty_days = 0;
  int64_t qty_hours = 0;
  int64_t qty_minutes = 0;
  int64_t qty_seconds = 0;

  if (!days_in_period.empty()) {
    qty_days = static_cast<int64_t>(std::stod(days_in_period));
  }

  if (!hours_in_period.empty()) {
    qty_hours = static_cast<int64_t>(std::stod(hours_in_period));
  }

  if (!minutes_in_period.empty()) {
    qty_minutes = static_cast<int64_t>(std::stod(minutes_in_period));
  }

  if (!seconds_in_period.empty()) {
    qty_seconds = static_cast<int64_t>(std::stod(seconds_in_period));
  }

  int64_t millis_in_the_period = qty_hours * 3600000 +  // millis in a hour
                                 qty_minutes * 60000 +  // millis in a minute
                                 qty_seconds * 1000;

  // It considers that a day has exactly 24 hours of duration
  int64_t days_to_standard_millis = 86400000;

  int64_t total_days = qty_days + (millis_in_the_period / days_to_standard_millis);
  int64_t module_millis = millis_in_the_period % days_to_standard_millis;

  // The response is a 64-bit integer where the lower half of the bytes represents the
  // number of the days and the other half represents the number of milliseconds.
  int64_t out = (total_days & 0x00000000FFFFFFFF);
  out |= ((module_millis << 32) & 0xFFFFFFFF00000000);

  *out_valid = true;
  return out;
}

// The operator will cast a generic string defined by the user into an interval of days.
// There are two formats of strings that are acceptable:
//   - The period in millis: '238398430'
//   - The period using a ISO8601 compatible format: 'P[n]Y[n]M[n]DT[n]H[n]M[n]S or P[n]W'
int64_t IntervalDaysHolder::operator()(ExecutionContext* ctx, const std::string& data,
                                       bool in_valid, bool* out_valid) {
  *out_valid = false;

  if (!in_valid) {
    return 0;
  }

  if (RE2::FullMatch(data, period_only_contains_numbers)) {
    return GetIntervalDayFromMillis(data, out_valid);
  }

  std::string period_in_weeks;
  if (RE2::FullMatch(data, iso8601_period_with_weeks, &period_in_weeks)) {
    return GetIntervalDayFromWeeks(period_in_weeks, out_valid);
  }

  std::string days_in_period;
  std::string hours_in_period;
  std::string minutes_in_period;
  std::string seconds_in_period;
  std::string ignored_string;  // string to store unnecessary captured groups
  if (std::regex_match(data, period_not_contains_time)) {
    if (RE2::FullMatch(data, iso8601_period_without_time, &ignored_string,
                       &ignored_string, &days_in_period)) {
      return GetIntervalDayFromCompletePeriod(days_in_period, hours_in_period,
                                              minutes_in_period, seconds_in_period,
                                              out_valid);
    }

    return_error(ctx, data);
    return 0;
  }

  if (RE2::FullMatch(data, iso8601_complete_period, &ignored_string, &ignored_string,
                     &days_in_period, &hours_in_period, &minutes_in_period,
                     &seconds_in_period)) {
    return GetIntervalDayFromCompletePeriod(
        days_in_period, hours_in_period, minutes_in_period, seconds_in_period, out_valid);
  }

  return_error(ctx, data);
  return 0;
}

Status IntervalDaysHolder::Make(const FunctionNode& node,
                                std::shared_ptr<IntervalDaysHolder>* holder) {
  const std::string function_name("castINTERVALDAY");
  return IntervalHolder<IntervalDaysHolder>::Make(node, holder, function_name);
}

Status IntervalDaysHolder::Make(int32_t suppress_errors,
                                std::shared_ptr<IntervalDaysHolder>* holder) {
  return IntervalHolder<IntervalDaysHolder>::Make(suppress_errors, holder);
}

Status IntervalYearsHolder::Make(const FunctionNode& node,
                                 std::shared_ptr<IntervalYearsHolder>* holder) {
  const std::string function_name("castINTERVALYEAR");
  return IntervalHolder<IntervalYearsHolder>::Make(node, holder, function_name);
}

Status IntervalYearsHolder::Make(int32_t suppress_errors,
                                 std::shared_ptr<IntervalYearsHolder>* holder) {
  return IntervalHolder<IntervalYearsHolder>::Make(suppress_errors, holder);
}

// The operator will cast a generic string defined by the user into an interval of months.
// There are two formats of strings that are acceptable:
//   - The period in millis: '238398430'
//   - The period using a ISO8601 compatible format: 'P[n]Y[n]M[n]DT[n]H[n]M[n]S or P[n]W'
int32_t IntervalYearsHolder::operator()(ExecutionContext* ctx, const std::string& data,
                                        bool in_valid, bool* out_valid) {
  *out_valid = false;

  if (!in_valid) {
    return 0;
  }

  if (RE2::FullMatch(data, period_only_contains_numbers)) {
    return GetIntervalYearFromNumber(data, out_valid);
  }

  std::string period_in_weeks;
  if (RE2::FullMatch(data, iso8601_period_with_weeks, &period_in_weeks)) {
    *out_valid = true;
    return 0;
  }

  std::string yrs_in_period;
  std::string months_in_period;
  std::string ignored_string;  // string to store unnecessary captured variables
  if (std::regex_match(data, period_not_contains_time)) {
    if (RE2::FullMatch(data, iso8601_period_without_time, &yrs_in_period,
                       &months_in_period, &ignored_string)) {
      return GetIntervalYearFromCompletePeriod(yrs_in_period, months_in_period,
                                               out_valid);
    }

    return_error(ctx, data);
    return 0;
  }

  if (RE2::FullMatch(data, iso8601_complete_period, &yrs_in_period, &months_in_period,
                     &ignored_string, &ignored_string, &ignored_string,
                     &ignored_string)) {
    return GetIntervalYearFromCompletePeriod(yrs_in_period, months_in_period, out_valid);
  }

  return_error(ctx, data);
  return 0;
}

int32_t IntervalYearsHolder::GetIntervalYearFromNumber(
    const std::string& number_as_string, bool* out_valid) {
  int32_t number_of_months = std::stoi(number_as_string);

  *out_valid = true;
  return number_of_months;
}

int32_t IntervalYearsHolder::GetIntervalYearFromCompletePeriod(
    const std::string& yrs_in_period, const std::string& months_in_period,
    bool* out_valid) {
  int32_t qty_yrs = 0;
  int32_t qty_months = 0;

  if (!yrs_in_period.empty()) {
    qty_yrs = static_cast<int32_t>(std::stod(yrs_in_period));
  }

  if (!months_in_period.empty()) {
    qty_months = static_cast<int32_t>(std::stod(months_in_period));
  }

  int32_t total_months = qty_yrs * 12 +  // qty months in an year
                         qty_months;

  *out_valid = true;
  return total_months;
}
}  // namespace gandiva
