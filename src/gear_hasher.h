#pragma once
#include <cstdint>
#include <stdint.h>

namespace rocksdb{
namespace titandb{

class GearHasher {
public:
	GearHasher(){};
	uint64_t Gear(uint8_t position);
};

} // namespace titandb
} // namespace rocksdb
