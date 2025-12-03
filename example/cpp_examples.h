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

#define HANDLE_ERROR(err_no)                  \
    do {                                      \
        if (err_no != 0) {                    \
            printf("get err no: %d", err_no); \
            return err_no;                    \
        }                                     \
    } while (0)

int demo_read();
int demo_write();
