/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "common/db_common.h"
#include "common/path.h"
#include "common/record.h"
#include "common/row_record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "reader/expression.h"
#include "reader/filter/filter.h"
#include "reader/qds_with_timegenerator.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_writer.h"
#include "cwrapper/tsfile_cwrapper.h"
#include "cwrapper/errno_define_c.h"
#include <cstdint>
#include <iostream>
#include <ostream>
#include <writer/tsfile_table_writer.h>
#include <string>
#include <filesystem>

#define HANDLE_ERROR(err_no)                  \
    do {                                      \
        if (err_no != 0) {                    \
            printf("get err no: %d", err_no); \
            return err_no;                    \
        }                                     \
    } while (0)

int main() {

    storage::libtsfile_init();

    std::string table_name = "table1";

    // Create a file with specify path to write tsfile.
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    mode_t mode = 0666;
    file.create("test_cpp.tsfile", flags, mode);

    // Create table schema to describe a table in a tsfile.
    auto* schema = new storage::TableSchema(
        table_name,
        {
            common::ColumnSchema("tag1", common::TSDataType::STRING, common::ColumnCategory::TAG),
            common::ColumnSchema("tag2", common::TSDataType::STRING, common::ColumnCategory::TAG),
            common::ColumnSchema("f1", common::TSDataType::INT32, common::ColumnCategory::FIELD),
            common::ColumnSchema("f2", common::TSDataType::INT64, common::ColumnCategory::FIELD),
            common::ColumnSchema("f3", common::TSDataType::FLOAT, common::ColumnCategory::FIELD),
            common::ColumnSchema("f4", common::TSDataType::DOUBLE, common::ColumnCategory::FIELD),
            common::ColumnSchema("f5", common::TSDataType::INT32),
            common::ColumnSchema("f6", common::TSDataType::INT64),
            common::ColumnSchema("f7", common::TSDataType::FLOAT),
            common::ColumnSchema("f8", common::TSDataType::DOUBLE),
        });

    // Create a file with specify path to write tsfile.
    auto* writer = new storage::TsFileTableWriter(&file, schema);

    // Create tablet to insert data.
    storage::Tablet tablet(
        table_name,
        {
            "tag1", 
            "tag2", 
            "f1", 
            "f2", 
            "f3", 
            "f4",
            "f5", 
            "f6", 
            "f7", 
            "f8",
        },
        {
            common::TSDataType::STRING, 
            common::TSDataType::STRING, 
            common::TSDataType::INT32, 
            common::TSDataType::INT64, 
            common::TSDataType::FLOAT, 
            common::TSDataType::DOUBLE, 
            common::TSDataType::INT32, 
            common::TSDataType::INT64, 
            common::TSDataType::FLOAT, 
            common::TSDataType::DOUBLE, 
        },
        {
            common::ColumnCategory::TAG, 
            common::ColumnCategory::TAG,
            common::ColumnCategory::FIELD,
            common::ColumnCategory::FIELD,
            common::ColumnCategory::FIELD,
            common::ColumnCategory::FIELD,
            common::ColumnCategory::FIELD,
            common::ColumnCategory::FIELD,
            common::ColumnCategory::FIELD,
            common::ColumnCategory::FIELD,
        },
        1024);


    for (int row = 0; row < 100; row++) {
        long timestamp = row;
        tablet.add_timestamp(row, timestamp);
        tablet.add_value(row, "tag1", "tag1");
        tablet.add_value(row, "tag2", "tag2");
        tablet.add_value(row, "f1", static_cast<int32_t>(row * -12345));
        tablet.add_value(row, "f2", static_cast<int64_t>(row * -12345));
        tablet.add_value(row, "f3", static_cast<float>(row * -12345.12345));
        tablet.add_value(row, "f4", static_cast<double>(row * -12345.12345));
        tablet.add_value(row, "f5", static_cast<int32_t>(row * 12345));
        tablet.add_value(row, "f6", static_cast<int64_t>(row * 12345));
        tablet.add_value(row, "f7", static_cast<float>(row * 12345.12345));
        tablet.add_value(row, "f8", static_cast<double>(row * 12345.12345));
    }

    // Write tablet data.
    HANDLE_ERROR(writer->write_table(tablet));

    // Flush data
    HANDLE_ERROR(writer->flush());

    // Close writer.
    HANDLE_ERROR(writer->close());

    delete writer;
    delete schema;

    return 0;
}
