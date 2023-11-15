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
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeAggregateFunction.h>

#include <AggregateFunctions/KeAggregateBitmapFunctions.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{
using namespace DB;

AggregateFunctionPtr
createKeAggregateBitmapOrCardinalityFunction(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);
    DataTypePtr argument_type_ptr = argument_types[0];
    AggregateFunctionPtr res(
        new local_engine::KeAggregateBitmapOrCardinality<Int64, local_engine::KeAggregateBitmapData<Int64>>(argument_type_ptr));

    if (!res)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument for aggregate function {}",
            argument_types[0]->getName(),
            name);

    return res;
}

AggregateFunctionPtr
createKeAggregateBitmapOrDataFunction(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);
    DataTypePtr argument_type_ptr = argument_types[0];
    AggregateFunctionPtr res(
        new local_engine::KeAggregateBitmapOr<std::string, local_engine::KeAggregateBitmapData<Int64>>(argument_type_ptr));

    if (!res)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument for aggregate function {}",
            argument_types[0]->getName(),
            name);

    return res;
}

AggregateFunctionPtr createKeAggregateBitmapAndValueFunction(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);
    DataTypePtr argument_type_ptr = argument_types[0];
    AggregateFunctionPtr res(
        new local_engine::KeAggregateBitmapAndValue<Int64, local_engine::KeAggregateBitmapData<Int64>>(argument_type_ptr));

    if (!res)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument for aggregate function {}",
            argument_types[0]->getName(),
            name);

    return res;
}

AggregateFunctionPtr createKeAggregateBitmapAndIdsFunction(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);
    DataTypePtr argument_type_ptr = argument_types[0];
    AggregateFunctionPtr res(
        new local_engine::KeAggregateBitmapAndIds<Int64, local_engine::KeAggregateBitmapData<Int64>>(argument_type_ptr));

    if (!res)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument for aggregate function {}",
            argument_types[0]->getName(),
            name);

    return res;
}

void registerKeAggregateFunctionsBitmap(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };
    factory.registerFunction("ke_bitmap_or_cardinality", {createKeAggregateBitmapOrCardinalityFunction, properties}, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("ke_bitmap_or_data", {createKeAggregateBitmapOrDataFunction, properties}, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("ke_bitmap_and_value", { createKeAggregateBitmapAndValueFunction, properties}, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("ke_bitmap_and_ids", {createKeAggregateBitmapAndIdsFunction, properties}, AggregateFunctionFactory::CaseInsensitive);
}
}
