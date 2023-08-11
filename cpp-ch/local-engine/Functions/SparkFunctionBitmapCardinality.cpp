/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "SparkFunctionBitmapCardinality.h"
#include <string>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Common/Exception.h>
#include "AggregateFunctions/KeAggregateBitmapData.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{
using namespace DB;
DB::DataTypePtr SparkFunctionBitmapCardinality::getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const
{
    if (arguments.size() != 1)
    {
        throw DB::Exception(
            DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires 1 arguments, passed {}", getName(), arguments.size());
    }

    if (!DB::WhichDataType(arguments[0].type).isString())
    {
        throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "All arguments for function {} must be String", getName());
    }
    return std::make_shared<DataTypeNumber<UInt64>>();
}

DB::ColumnPtr SparkFunctionBitmapCardinality::executeImpl(
    const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, const size_t input_rows_count) const
{
    auto x = arguments[0].column;

    auto res = result_type->createColumn();
    res->reserve(input_rows_count);

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        auto data_str = x->getDataAt(i);
        if (data_str.empty())
        {
            res->insertDefault();
        }
        else
        {
            auto charBuff = std::make_unique<ReadBufferFromJavaBitmap>(const_cast<char *>(data_str.data), data_str.size);
            auto bitmap = std::make_unique<KeRoaringBitmapData<Int64>>();
            bitmap->read(*charBuff);
            res->insert(bitmap->size());
        }
    }
    return res;
}

REGISTER_FUNCTION(SparkFunctionBitmapCardinality)
{
    factory.registerFunction<SparkFunctionBitmapCardinality>();
}
}
