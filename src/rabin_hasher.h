#pragma once
#include <vector>
#include <stdint.h>


namespace rocksdb {
namespace titandb {


class RabinHasher {
	public: 
		RabinHasher(){}
		static uint64_t RabinHash(std::vector<uint64_t> vec) {
			__uint128_t ads = 1;
			__uint128_t res = 0;
			uint64_t base = 100007;
			uint64_t mod_base = 0xFFFFFFFFFFFFFFFF;
			for(uint64_t x : vec) {
				res = (res + (__uint128_t)x * ads) % mod_base;
				ads = (ads * base) % mod_base;
			}
			return (uint64_t) res;
		}
};
}
}
