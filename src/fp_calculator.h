#pragma once
#include <cstdint>
#include <vector>
#include "gear_hasher.h"
#include "rabin_hasher.h"
#include "string.h"
#include "titan/debug_info.h"

namespace rocksdb {
namespace titandb {

class FPCalculator {
public:
	FPCalculator(int feature_num, int shiftbits, int super_feature_num, uint64_t mask,
		std::vector<uint64_t> mi, std::vector<uint64_t> ai)
		:feature_num_(feature_num),
		shiftbits_(shiftbits),
		super_feature_num_(super_feature_num),
		mask_(mask),
		mi_(mi),
		ai_(ai){}

	FPCalculator(const FPCalculator&) = delete;

	std::vector<uint64_t> CalCulateFP(const char* target) {
		std::vector<uint64_t> result(feature_num_);
		uint64_t fp = 0;
		GearHasher hasher;
		const uint8_t* content = (uint8_t*)target;
		uint32_t length = strlen(target);
		for(uint32_t i = 0; i < length; i++) {
		    fp <<= shiftbits_;
			fp += hasher.Gear((uint8_t)(content[i]));
			if((fp & mask_) == 0) {
				for(int j = 0; j < feature_num_; j++) {
					uint64_t tmp = ((__uint128_t)mi_[j] * fp + ai_[j]) % ((uint64_t)1 << 32);
					if(result[j] < tmp) { result[j] = tmp; }
				}
			}
		}
		std::vector<uint64_t> super_result(super_feature_num_);
		int super_influ = feature_num_ / super_feature_num_;
		for(int i = 0; i < super_feature_num_; i++) {
			int start = i * super_influ;
			int end = (i + 1) * super_influ - 1;
			if(i == super_feature_num_ - 1) end = feature_num_ - 1;
			// rabin fingerprint
			std::vector<uint64_t> base;
			for(int j = start; j <= end; j++) {
				base.push_back(result[j]);
			}
			super_result[i] = RabinHasher::RabinHash(base);	
		}
		return super_result;
	}

	void GetBasicInfo() {
		Debugger debugger("|fp_calculator.h|GetBasicInfo:");
		debugger.dprintln("feature_num: %d, super_feature_num: %d, shiftbit: %d, mask: %llu, mi: %llu, %llu, %llu", feature_num_, 
				super_feature_num_, shiftbits_, mask_, mi_[0], mi_[1], mi_[2]);

	}

private:
		int feature_num_;
		int shiftbits_;
		int super_feature_num_;
		uint64_t mask_;
		std::vector<uint64_t> mi_, ai_;
};
}
}
