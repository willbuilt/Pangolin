#pragma once

#include <memory>
#include <thread>
#include <mutex>
#include <functional>
#include <fstream>
#include <queue>
#include <condition_variable>

#include <mio/mmap.hpp>
#include <pangolin/utils/file_utils.h>

namespace pangolin
{

// A thread safe queued file wrapper
class RandomFile
{
public:
    enum class GetPolicy
    {
        Throw,
        Grow,
        Wait
    };

    RandomFile(const RandomFile&) = delete;

    RandomFile(const std::string& filename)
        : filename(filename), should_run(true), fd(mio::invalid_handle)
    {
        if(FileExists(filename)) {
            write_thread = std::thread(&RandomFile::WriteThread, this);
        }else{
            throw std::runtime_error("No such file");
        }
    }

    ~RandomFile()
    {
        should_run = false;
        queue_cond.notify_all();
        write_thread.join();
    }

    // Atomically stream to file (and jump the queue)
    void Append(const std::function<void(std::ostream&)>& func)
    {
        std::unique_lock<std::mutex> l(write_mutex);
        func(file);
    }

    void Append(const std::shared_ptr<uint8_t>& data, size_t size_bytes)
    {
        std::unique_lock<std::mutex> lq(queue_mutex);
        to_write.emplace([data, size_bytes, this](){
            DirectWrite(data.get(), size_bytes);
        });
        queue_cond.notify_one();
    }

    std::shared_ptr<uint8_t>
    Get(size_t offset_bytes, size_t size_bytes, GetPolicy policy = GetPolicy::Throw)
    {
        if(!mmap || mmap->size() < size_bytes) {
            std::error_code err;

            fd = mio::detail::open_file(filename, mio::access_mode::write, err);
            if(err) throw std::runtime_error(err.message());

            size_t file_size = mio::detail::query_file_size(fd, err);
            if(err) throw std::runtime_error(err.message());

            if(offset_bytes + size_bytes > file_size) {
                std::unique_lock<std::mutex> lw(write_mutex);

                // Can't map this right now
                if(policy == GetPolicy::Throw) {
                    throw std::runtime_error("Get() called out of allocated file size.");
                }else if(policy == GetPolicy::Wait) {
                    while(offset_bytes + size_bytes > file_size) {
                        write_cond.wait(lw);
                        file_size = mio::detail::query_file_size(fd, err);
                        if(err) throw std::runtime_error(err.message());
                        std::cout << file_size << std::endl;
                    }
                }else if(policy == GetPolicy::Grow){
                    const size_t new_size_bytes = offset_bytes + size_bytes;
                    Truncate(new_size_bytes, err);
                    if(err) throw std::runtime_error(err.message());
                    file.seekp(new_size_bytes, std::ios_base::beg);
                }
            }

            // We either don't have a mapping yet or it is partially mapped only
            if(offset_bytes + size_bytes < bytes_written) {
                // Create a new mmap.
                // The old will be freed when it is no longer references
                mmap = std::make_shared<mio::ummap_sink>();
                mmap->map(fd, 0, mio::map_entire_file, err);
                if(err) throw std::runtime_error(err.message());
            }
        }

        uint8_t* user_ptr =  mmap->data() + offset_bytes;
        return std::shared_ptr<uint8_t>(mmap, user_ptr);
    }

private:   
    void DirectWrite(uint8_t* data, size_t size_bytes)
    {
        std::unique_lock<std::mutex> lw(write_mutex);
        file.write((char*)data, size_bytes);
        bytes_written += size_bytes;
        write_cond.notify_one();
    }

    void Truncate(size_t size_bytes, std::error_code& )
    {
        ftruncate(fd, size_bytes);
    }

    void WriteThread()
    {       
        file.open(filename, std::ios_base::binary);

        while(true)
        {
            while(should_run && to_write.empty()) {
                std::unique_lock<std::mutex> lq(queue_mutex);
                queue_cond.wait(lq);
            }

            if(!to_write.empty()) {
                // Execute the functor at the front of the queue
                to_write.front()();
                std::unique_lock<std::mutex> lq(queue_mutex);
                to_write.pop();
            }else if(!should_run) {
                break;
            }
        }
    }

    std::string filename;
    volatile bool should_run;
    std::thread write_thread;

    std::condition_variable queue_cond;
    std::condition_variable write_cond;
    std::mutex queue_mutex;
    std::mutex write_mutex;

    std::ofstream file;
    size_t bytes_written;
    size_t bytes_queued;
    std::queue<std::function<void(void)>> to_write;

    // Seperate file descriptor for memory mapped reading.
    mio::file_handle_type fd;
    std::shared_ptr<mio::ummap_sink> mmap;
};

}
