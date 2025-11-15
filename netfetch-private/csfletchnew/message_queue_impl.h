#ifndef MESSAGE_QUEUE_IMPL_H
#define MESSAGE_QUEUE_IMPL_H

#include "message_queue.h"
#include <mutex>

template<class obj_t>
MessagePtrQueue<obj_t>::MessagePtrQueue() {}

template<class obj_t>
MessagePtrQueue<obj_t>::MessagePtrQueue(uint32_t size) {
    this->init(size);
}

template<class obj_t>
void MessagePtrQueue<obj_t>::init(uint32_t size) {
    this->_size = size;
    this->_obj_ptrs = new obj_t*[size];
    for (size_t i = 0; i < size; i++) {
        this->_obj_ptrs[i] = NULL;
    }
}

template<class obj_t>
bool MessagePtrQueue<obj_t>::write(obj_t* newobj) {
    static std::mutex write_mutex;  // 保证线程安全的写操作
    std::lock_guard<std::mutex> lock(write_mutex);

    if (((this->_head + 1) % this->_size) != this->_tail) {
        this->_obj_ptrs[this->_head] = newobj;
        this->_head = (this->_head + 1) % this->_size;
        return true;
    }
    return false;  // 队列满
}

template<class obj_t>
bool MessagePtrQueue<obj_t>::single_write(obj_t* newobj) {
    static std::mutex single_write_mutex;  // 保证线程安全的写操作
    std::lock_guard<std::mutex> lock(single_write_mutex);

    if (((this->_head + 1) % this->_size) == this->_tail) {
        this->_tail = (this->_tail + 1) % this->_size;  // 覆盖最旧的元素
    }
    this->_obj_ptrs[this->_head] = newobj;
    this->_head = (this->_head + 1) % this->_size;
    return true;
}

template<class obj_t>
obj_t* MessagePtrQueue<obj_t>::read() {
    static std::mutex read_mutex;  // 保证线程安全的读操作
    std::lock_guard<std::mutex> lock(read_mutex);

    obj_t* result = NULL;
    if (this->_tail != this->_head) {
        result = this->_obj_ptrs[this->_tail];
        this->_obj_ptrs[this->_tail] = NULL;
        this->_tail = (this->_tail + 1) % this->_size;
    }
    return result;
}

template<class obj_t>
MessagePtrQueue<obj_t>::~MessagePtrQueue() {
    for (size_t i = 0; i < this->_size; i++) {
        if (this->_obj_ptrs[i] != NULL) {
            delete this->_obj_ptrs[i];
            this->_obj_ptrs[i] = NULL;
        }
    }
    delete[] this->_obj_ptrs;
    this->_obj_ptrs = NULL;
}

#endif
