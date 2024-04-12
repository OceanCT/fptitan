#pragma once

#include <algorithm>
#include <cstdint>
#include <mutex>
#include <stdint.h>
#include <string>
#include <sys/types.h>
#include <vector>
#include <map>
#include <set>
#include "blob_format.h"
#include "rocksdb/slice.h"
#include "fp_calculator.h"
#include "titan/debug_info.h"
#include "titan/options.h"

namespace rocksdb {
namespace titandb {

typedef std::vector<uint64_t> FPFeatures;

class FPIndex{
 public:
	FPIndex(int max_size, int feature_num, int shiftbits, int super_feature_num, 
			uint64_t mask, int max_match, std::vector<uint64_t> mi, std::vector<uint64_t> ai)
		:max_size_(max_size), 
		super_feature_num_(super_feature_num), 
		max_match_(max_match),		
		fp_calculator(new FPCalculator(feature_num, shiftbits, super_feature_num, mask, mi, ai)){
			fp_index.resize(super_feature_num_);
			valid_add_record_cnt = 0;
			valid_find_similar_cnt = 0;
			found_record_cnt = 0;
			successful_cnt = 0;
		}
	FPIndex(const TitanCFOptions& cf_options) 
		:max_size_(cf_options.fp_index_max_size),
		super_feature_num_(cf_options.fp_super_feature_num),
		max_match_(cf_options.fp_max_match),
		fp_calculator(new FPCalculator(cf_options.fp_feature_num, cf_options.fp_shiftbits,
					cf_options.fp_super_feature_num, cf_options.fp_mask, cf_options.fp_mi, cf_options.fp_ai)){
			fp_index.resize(super_feature_num_);
			valid_add_record_cnt = 0;
			valid_find_similar_cnt = 0;
			found_record_cnt = 0;
			successful_cnt = 0;
		}

	void AddRecord(std::string key, FPFeatures fp_features, BlobIndex blob_index) {
		mu.lock();
		if(size_ == max_size_) {
			mu.unlock();
			return;
		}
		removeKey(key);	
		for(int i = 0; i < super_feature_num_; i++) {
			fp_index[i][fp_features[i]].insert(key);
			fp_index_ref[key].push_back({i, fp_features[i]});
		}
		key_blob_index[key] = blob_index;
		size_++;
		valid_add_record_cnt++;	
		Debugger debugger("|fp_index.h|AddRecord");
		debugger.dprintln("AddRecord key: %s, fp_features: %llu, %llu, %llu", key.c_str(), fp_features[0], fp_features[1], fp_features[2]);

		mu.unlock();
	}

	void FindSimilarRecords(std::string key, std::string value, std::vector<std::pair<std::string, BlobIndex>>& similar_records) {
		Debugger debugger("|fp_index.h|FindSimilarRecords");
		mu.lock();
		std::vector<uint64_t> fingerprints = fp_calculator->CalCulateFP(value.c_str());
		if(fp_index_ref.count(key)) {
			removeKey(key);
			size_--;
		}
		std::set<std::string> similar_records_keys;
		for(int i = 0; i < super_feature_num_; i++) {
			for(auto similar_key: fp_index[i][fingerprints[i]])	{
				similar_records_keys.insert(similar_key);
				if((int)similar_records_keys.size() == max_match_) break;
			}	
			if((int)similar_records_keys.size() == max_match_) break;
		}
		for(std::string similar_key: similar_records_keys) {
			similar_records.push_back({similar_key, key_blob_index[similar_key]});
			removeKey(similar_key);
		}
		size_ -= similar_records.size();
		valid_find_similar_cnt++;
		found_record_cnt += similar_records.size();
		debugger.dprintln("Index Size: %d, MaxMatch: %d, FindSimilarRecordsSize: %d, AddCnt: %d, FoundRecordCnt: %d, SuccessfulCnt: %d, value: %s", 
				size_, max_match_, similar_records.size(), valid_add_record_cnt, found_record_cnt, successful_cnt, value.c_str());
		mu.unlock();
	}

	FPFeatures CalculateFP(const Slice& value) {
		return fp_calculator->CalCulateFP(value.ToString().c_str());
	}

	void AddSuccessfulFP(int cnt) {
		mu.lock();
		successful_cnt += cnt;
		mu.unlock();
	}

	void AddFoundRecord(int cnt) {
		mu.lock();
		found_record_cnt += cnt;
		mu.unlock();
	}

	std::string GetFPInfo() {
		std::string message = "";
		mu.lock();
		message += "ValidAddRecordCnt:" + std::to_string(valid_add_record_cnt);
		message += ";ValidFindSimilarCnt:" + std::to_string(valid_find_similar_cnt);
		message += ";SuccessfulCnt:" + std::to_string(successful_cnt);
		message += ";FoundRecordCnt:" + std::to_string(found_record_cnt);
		mu.unlock();
		return message;
	}

	int GetSize() { return size_; }

    void GetBasicInfo() {
		fp_calculator->GetBasicInfo();
	}

 private:
	int max_size_;
	int size_ = 0;
	int super_feature_num_;
	int max_match_;
	// superfeature index -> [(fingerprint) -> record: key]
	std::vector<std::map<uint64_t, std::set<std::string>>> fp_index;
	// record: [key] -> [superfeature index, fingerprint] 
	std::map<std::string, std::vector<std::pair<int, uint64_t>>> fp_index_ref;

	std::mutex mu;

	FPCalculator* fp_calculator;
	
	std::map<std::string, BlobIndex> key_blob_index;

	unsigned long long valid_add_record_cnt;
	unsigned long long valid_find_similar_cnt;
	unsigned long long successful_cnt; 
	unsigned long long found_record_cnt;

	void removeKey(const Slice& key) {
		// remove key from fp_index
		for(auto tmp: fp_index_ref[key.ToString()]) {
			auto super_feature_index = tmp.first;
			auto fingerprint = tmp.second; 
			fp_index[super_feature_index][fingerprint].erase(key.ToString());	
		}
		// remove key from fp_index_ref
		fp_index_ref.erase(key.ToString());	
		// remove key from key_blob_index
		key_blob_index.erase(key.ToString());	
	}
};

}
}
