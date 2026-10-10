// RN-142 cases 44/45; test-only, no product source changes.
#include "dataset/dataset_index.h"
#include "crosslang_expected.h"
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>
using namespace storage::dataset;
namespace expected = rn142_expected;

static std::vector<unsigned char> read_bytes(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("Cannot read fixture: " + path);
    return std::vector<unsigned char>(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}
static uint64_t stat_fingerprint(const struct stat& st) {
    const uint64_t words[] = {static_cast<uint64_t>(st.st_size),
        static_cast<uint64_t>(st.st_mtim.tv_sec)*1000000000ULL + static_cast<uint64_t>(st.st_mtim.tv_nsec)};
    uint64_t hash = 1469598103934665603ULL;
    for (uint64_t word : words) for (unsigned n=0; n<8; ++n) {
        hash ^= (word >> (n*8)) & 255ULL;
        hash *= 1099511628211ULL;
    }
    return hash;
}

TEST(DatasetIndexCrossLangTest, OpensPythonIndexAndResolvesIds) {
    const char* root = std::getenv("RN142_CROSSLANG_DIR");
    ASSERT_NE(nullptr, root);
    const std::string path = std::string(root) + "/python-d1.tsidx";
    const auto raw = read_bytes(path);
    ASSERT_GE(raw.size(), 64U);
    MappedDatasetIndex index;
    ASSERT_EQ(DatasetIndexStatus::OK, index.open(path)) << index.error_message();
    ASSERT_TRUE(index.is_open());
    ASSERT_NE(nullptr, index.header());
    EXPECT_EQ(0, std::memcmp(index.header(), expected::kHeader, 64));
    EXPECT_EQ(13U, index.header()->section_count);
    EXPECT_EQ(raw.size(), index.file_length());
    for (size_t n=0; n<std::size(expected::kSections); ++n) {
        const auto& want = expected::kSections[n];
        const size_t pos = index.header()->directory_offset + n*index.header()->directory_entry_size;
        ASSERT_LE(pos + sizeof(DatasetIndexDirectoryEntry), raw.size());
        DatasetIndexDirectoryEntry entry;
        std::memcpy(&entry, raw.data()+pos, sizeof(entry));
        EXPECT_EQ(want.type, entry.section_type); EXPECT_EQ(want.record_size, entry.record_size);
        EXPECT_EQ(want.offset, entry.offset); EXPECT_EQ(want.length, entry.length);
        EXPECT_EQ(want.count, entry.count); EXPECT_EQ(want.crc, entry.crc32c);
        DatasetIndexSectionView view;
        ASSERT_EQ(DatasetIndexStatus::OK, index.section(static_cast<DatasetIndexSectionType>(want.type),view));
        EXPECT_EQ(want.record_size, view.record_size); EXPECT_EQ(want.count, view.count);
        EXPECT_EQ(want.length, view.length);
        EXPECT_EQ(want.crc, dataset_index_crc32c(view.data,view.length));
        std::cout << "section=" << want.type << " count=" << view.count << " crc=" << want.crc << '\n';
    }
    DatasetIndexSectionView file_view;
    ASSERT_EQ(DatasetIndexStatus::OK,index.section(DatasetIndexSectionType::TSFILE_RECORD,file_view));
    ASSERT_EQ(std::size(expected::kFiles), file_view.count);
    for (const auto& want : expected::kFiles) {
        const TsFileRecord* file = nullptr;
        ASSERT_EQ(DatasetIndexStatus::OK,index.record(DatasetIndexSectionType::TSFILE_RECORD,want.id,file));
        ASSERT_NE(nullptr,file);
        EXPECT_EQ(want.sid,file->path_sid); EXPECT_EQ(want.size,file->file_size);
        EXPECT_EQ(want.fingerprint,file->file_fingerprint);
        DatasetIndexStringView name;
        ASSERT_EQ(DatasetIndexStatus::OK,index.string(file->path_sid,name));
        EXPECT_EQ(std::string(want.path),name.to_string());
        struct stat st {};
        ASSERT_EQ(0,::stat(want.path,&st));
        EXPECT_EQ(static_cast<uint64_t>(st.st_size),file->file_size);
        EXPECT_EQ(stat_fingerprint(st),file->file_fingerprint);
        std::cout << "file=" << want.path << " size=" << st.st_size << " fingerprint=" << file->file_fingerprint << '\n';
    }
    for (const auto& want : expected::kTables) {
        std::vector<uint32_t> ids;
        ASSERT_EQ(DatasetIndexStatus::OK,index.find_table_ids(want.name,ids));
        EXPECT_EQ(want.ids,ids);
    }
    for (const auto& want : expected::kDevices) {
        uint32_t id = UINT32_MAX;
        ASSERT_EQ(DatasetIndexStatus::OK,index.find_device_id(want.parent,want.name,id));
        EXPECT_EQ(want.id,id) << want.name;
        std::cout << "device=" << want.name << " id=" << id << '\n';
    }
    for (const auto& want : expected::kColumns) {
        uint32_t id = UINT32_MAX;
        ASSERT_EQ(DatasetIndexStatus::OK,index.find_column_id(want.parent,want.name,id));
        EXPECT_EQ(want.id,id) << want.name;
        std::cout << "column=" << want.name << " id=" << id << '\n';
    }
    for (const auto& want : expected::kSeries) {
        uint32_t id = UINT32_MAX;
        ASSERT_EQ(DatasetIndexStatus::OK,index.find_series_id(want.device,want.column,id));
        EXPECT_EQ(want.id,id);
        std::cout << "series device=" << want.device << " column=" << want.column << " id=" << id << '\n';
    }
    RecordProperty("case",44); RecordProperty("sections",13);
    RecordProperty("file_records",static_cast<int>(std::size(expected::kFiles)));
    RecordProperty("series_lookups",static_cast<int>(std::size(expected::kSeries)));
    index.close(); EXPECT_FALSE(index.is_open());
}

TEST(DatasetIndexCrossLangTest, RejectsDamagedIndexWithSpecificStatus) {
    const char* root = std::getenv("RN142_CROSSLANG_DIR"); ASSERT_NE(nullptr,root);
    const std::string directory(root);
    MappedDatasetIndex index;
    ASSERT_EQ(DatasetIndexStatus::OK,index.open(directory+"/two-tables-valid.tsidx")) << index.error_message();
    DatasetIndexSectionView tables;
    ASSERT_EQ(DatasetIndexStatus::OK,index.section(DatasetIndexSectionType::TABLE_RECORD,tables));
    ASSERT_EQ(2U,tables.count);
    index.close();
    struct Damage {const char* name; DatasetIndexStatus status; const char* error;};
    const Damage variants[] = {
        {"bad-magic",DatasetIndexStatus::BAD_MAGIC,"bad dataset index magic"},
        {"bad-version",DatasetIndexStatus::UNSUPPORTED_VERSION,"unsupported dataset index version"},
        {"bad-header-crc",DatasetIndexStatus::BAD_CHECKSUM,"header checksum"},
        {"truncated",DatasetIndexStatus::BAD_HEADER,"shorter than header"},
        {"bad-section-crc",DatasetIndexStatus::BAD_CHECKSUM,"section checksum"},
        {"duplicate-table-name",DatasetIndexStatus::BAD_REFERENCE,"duplicate table names"},
    };
    for (const auto& want : variants) {
        SCOPED_TRACE(want.name);
        const auto actual = index.open(directory+"/"+want.name+".tsidx");
        EXPECT_EQ(want.status,actual) << index.error_message();
        EXPECT_FALSE(index.is_open()); EXPECT_FALSE(index.error_message().empty());
        EXPECT_NE(std::string::npos,index.error_message().find(want.error));
        std::cout << "damage=" << want.name << " status=" << dataset_index_status_name(actual)
                  << " is_open=" << index.is_open() << " error=" << index.error_message() << '\n';
    }
    RecordProperty("case",45); RecordProperty("damage_variants",6);
    // Preserve all damaged files and logs for review.
}
