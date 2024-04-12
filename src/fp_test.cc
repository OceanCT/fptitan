#include "blob_format.h"
#include "fp_index.h"
#include "rocksdb/slice.h"
#include "titan/options.h"
#include "rocksdb/slice.h"
#include <gtest/gtest.h>
#include <utility>

namespace rocksdb {
namespace titandb {
class FPTest : public testing::Test {};

TEST(FPIndexTest, AddKV) {
	auto fp_index = new rocksdb::titandb::FPIndex(rocksdb::titandb::TitanCFOptions());
	rocksdb::Slice key("asfasdf"), value("asdfad");
	rocksdb::titandb::BlobIndex blob_index;
	blob_index.file_number = 3;
	blob_index.blob_handle.offset = 4;
	auto fps = fp_index->CalculateFP(value);
	fp_index->AddRecord(key.ToString(), fps, blob_index);

	rocksdb::Slice key1("asdfad");
	std::vector<std::pair<std::string, rocksdb::titandb::BlobIndex>> similar_records;
	fp_index->FindSimilarRecords(key1.ToString(), value.ToString(), similar_records);
	for(auto record: similar_records) {
		std::printf("%ld %ld\n", record.second.file_number, record.second.blob_handle.offset);
		std::printf("%s\n", fp_index->GetFPInfo().c_str());
	}
}
}
}

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv); 
	return RUN_ALL_TESTS();	
}


