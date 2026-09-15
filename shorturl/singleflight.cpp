#include "singleflight.h"

SingleFlight::SingleFlight()
    : max_inflight_(1024), max_waiters_per_key_(256) {}

void SingleFlight::configure(size_t max_inflight, size_t max_waiters_per_key) {
    std::lock_guard<std::mutex> guard(mutex_);
    max_inflight_ = max_inflight == 0 ? 1 : max_inflight;
    max_waiters_per_key_ = max_waiters_per_key == 0 ? 1 : max_waiters_per_key;
}

size_t SingleFlight::inflight_count() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return inflight_.size();
}

SingleFlightResult
SingleFlight::do_flight(const std::string& key,
                        const std::function<SingleFlightResult()>& loader) {
    std::shared_ptr<Call> call;
    bool am_leader = false;

    {
        std::lock_guard<std::mutex> guard(mutex_);
        auto it = inflight_.find(key);
        if (it != inflight_.end()) {
            call = it->second;
            if (call->waiters >= max_waiters_per_key_) {
                SingleFlightResult overload;
                overload.kind = SingleFlightResult::Kind::Overload;
                overload.error = "singleflight waiters overflow";
                return overload;
            }
            ++call->waiters;
        } else {
            if (inflight_.size() >= max_inflight_) {
                SingleFlightResult overload;
                overload.kind = SingleFlightResult::Kind::Overload;
                overload.error = "singleflight inflight overflow";
                return overload;
            }
            call = std::make_shared<Call>();
            inflight_[key] = call;
            am_leader = true;
        }
    }

    if (!am_leader) {
        std::unique_lock<std::mutex> lock(call->mu);
        call->cv.wait(lock, [&] { return call->done; });
        SingleFlightResult shared = call->result;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            if (call->waiters > 0) {
                --call->waiters;
            }
        }
        return shared;
    }

    // Leader: map entry registered; run loader without holding map or call lock.
    SingleFlightResult result;
    try {
        result = loader();
    } catch (...) {
        result.kind = SingleFlightResult::Kind::Error;
        result.error = "singleflight loader exception";
    }

    {
        std::lock_guard<std::mutex> lock(call->mu);
        call->result = result;
        call->done = true;
    }
    call->cv.notify_all();

    {
        std::lock_guard<std::mutex> guard(mutex_);
        auto it = inflight_.find(key);
        if (it != inflight_.end() && it->second == call) {
            inflight_.erase(it);
        }
    }

    return result;
}
