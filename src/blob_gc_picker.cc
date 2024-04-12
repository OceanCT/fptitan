#include "blob_gc_picker.h"
#include "titan/debug_info.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <cinttypes>

#include "titan_logging.h"

namespace rocksdb {
namespace titandb {

BasicBlobGCPicker::BasicBlobGCPicker(TitanDBOptions db_options,
                                     TitanCFOptions cf_options,
                                     TitanStats* stats)
    : db_options_(db_options), cf_options_(cf_options), stats_(stats) {}

BasicBlobGCPicker::~BasicBlobGCPicker() {}

std::unique_ptr<BlobGC> BasicBlobGCPicker::PickBlobGC(
    BlobStorage* blob_storage) {
  Debugger debugger("|blob_gc_picker.cc|PickBlobGC");
  debugger.dprintln("trying to pick gc job...");
  Status s;
  std::vector<std::shared_ptr<BlobFileMeta>> blob_files;

  uint64_t batch_size = 0;
  uint64_t estimate_output_size = 0;
  bool stop_picking = false;
  bool maybe_continue_next_time = false;
  uint64_t next_gc_size = 0;
  for (auto& gc_score : blob_storage->gc_score()) {
	  blob_storage->ComputeGCScore();
    debugger.dprintln("blob file[%d]'s gc score is: %lf", gc_score.file_number, gc_score.score);
	if (gc_score.score < cf_options_.blob_file_discardable_ratio) {
      break;
    }
    auto blob_file = blob_storage->FindFile(gc_score.file_number).lock();
    if (!CheckBlobFile(blob_file.get())) {
      // Skip this file id this file is being GCed
      // or this file had been GCed
	  if(blob_file == nullptr) {
		debugger.dprintln("blob file[%d] is null", gc_score.file_number);
	  } else if(blob_file->file_state() != BlobFileMeta::FileState::kNormal) {
		debugger.dprintln("blob file[%d] current state: %d", gc_score.file_number, blob_file->file_state());
	  }

      TITAN_LOG_INFO(db_options_.info_log, "Blob file %" PRIu64 " no need gc",
                     blob_file->file_number());
      continue;
    }
	// debugger.dprintln("blob file[%d], stop_picking flag: %d", gc_score.file_number, stop_picking);
    if (!stop_picking) {
      blob_files.emplace_back(blob_file);
      if (blob_file->file_size() <= cf_options_.merge_small_file_threshold) {
        RecordTick(statistics(stats_), TITAN_GC_SMALL_FILE, 1);
      } else {
        RecordTick(statistics(stats_), TITAN_GC_DISCARDABLE, 1);
      }
      batch_size += blob_file->file_size();
      estimate_output_size += blob_file->live_data_size();
      if (batch_size >= cf_options_.max_gc_batch_size ||
          estimate_output_size >= cf_options_.blob_file_target_size) {
        // Stop pick file for this gc, but still check file for whether need
        // trigger gc after this
		debugger.dprintln("batch size: %d, max_gc_batch_size: %d, estimate_output_size: %d, blob_file_target_size: %d",
				batch_size, cf_options_.max_gc_batch_size, estimate_output_size, cf_options_.blob_file_target_size);
		if(blob_files.size() > 1) stop_picking = true;
      }
    } else {
      next_gc_size += blob_file->file_size();
      if (next_gc_size > cf_options_.min_gc_batch_size) {
        maybe_continue_next_time = true;
        RecordTick(statistics(stats_), TITAN_GC_REMAIN, 1);
        TITAN_LOG_INFO(db_options_.info_log,
                       "remain more than %" PRIu64
                       " bytes to be gc and trigger after this gc",
                       next_gc_size);
        break;
      }
    }
  }
  TITAN_LOG_DEBUG(db_options_.info_log,
                  "got batch size %" PRIu64 ", estimate output %" PRIu64
                  " bytes",
                  batch_size, estimate_output_size);
  if (blob_files.empty() ||
      (batch_size < cf_options_.min_gc_batch_size &&
       estimate_output_size < cf_options_.blob_file_target_size)) {
	debugger.dprintln("blob file num: %d", blob_files.size());
	return nullptr;
  }
  debugger.dprintln("titan gc picker, blob_files[0].GetDiscardableRatio: %lf", blob_files[0]->GetDiscardableRatio());
  // if there is only one small file to merge, no need to perform
  if (blob_files.size() == 1 &&
      blob_files[0]->file_size() <= cf_options_.merge_small_file_threshold &&
      blob_files[0]->GetDiscardableRatio() <
          cf_options_.blob_file_discardable_ratio) {
	debugger.dprintln("No GC, blob_files_size: %d", blob_files.size());
    return nullptr;
  }

  return std::unique_ptr<BlobGC>(new BlobGC(
      std::move(blob_files), std::move(cf_options_), maybe_continue_next_time));
}

bool BasicBlobGCPicker::GCFinished(BlobStorage* blob_storage) {
  Debugger debugger("blob_gc_picker.cc|BasicBlobGCPicker.GCFinished");
  debugger.dprintln("check whether gc has finished");
  Status s;
  std::vector<std::shared_ptr<BlobFileMeta>> blob_files;
  blob_storage->ComputeGCScore();
  debugger.dprintln("gc score size: %d", blob_storage->gc_score().size());
  for(auto& gc_score : blob_storage->gc_score()) {
  	debugger.dprintln("gc_score of file[%d]: %lf", gc_score.file_number, gc_score.score);
	auto blob_file = blob_storage->FindFile(gc_score.file_number).lock();
  	if(blob_file == nullptr) {
	  debugger.dprintln("blob_file[%d] does not exist", gc_score.file_number);
	  return false;
	}
    if(blob_file->file_state() != BlobFileMeta::FileState::kNormal) {
	  debugger.dprintln("blob_file[%d] state: %d", gc_score.file_number, blob_file->file_state());
	  return false;
	}
    if (gc_score.score >= cf_options_.blob_file_discardable_ratio) { 
	  debugger.dprintln("blob_file[%d] gc_score: %lf", gc_score.file_number, gc_score.score);
	  return false;
	}
  }	
  return true;
}


bool BasicBlobGCPicker::CheckBlobFile(BlobFileMeta* blob_file) const {
  assert(blob_file == nullptr ||
         blob_file->file_state() != BlobFileMeta::FileState::kNone);
  if (blob_file == nullptr ||
      blob_file->file_state() != BlobFileMeta::FileState::kNormal)
    return false;

  return true;
}

}  // namespace titandb
}  // namespace rocksdb
