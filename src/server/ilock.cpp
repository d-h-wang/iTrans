#include "ilock.h"
#include "trans_mgr.h"
#include "db_worker.h"
#include <co_routine_inner.h>

#define ATOMIC_CAS(val, cmpv, newv) __sync_val_compare_and_swap((val), (cmpv), (newv))

#define OWN(A, B) \
  (((A) & (MASK_OWNER)) == ((B) & (MASK_OWNER)))

#define EXCLUSIVE_MODE(A) \
  (0 != ((A) & MASK_EXCLUSIVE))

__thread ThreadLockTable* ILock::lock_table_;
__thread int64_t ILock::thread_index_;
Worker* ILock::host_;

int ThreadLockTable::get_write_waiter(uint64_t uid, 
    stCoRoutine_t* &routinue) {
  auto entry = wr_wait_list_.find(uid);
  if( entry == wr_wait_list_.end() ) return 0;
  routinue = entry->second.front()->routinue_;
  entry->second.pop_front();
  if( entry->second.empty() ) {
    wr_wait_list_.erase(entry);
    return 0;
  }
  return 1;
}

int ThreadLockTable::get_read_waiter(uint64_t uid, 
    vector<stCoRoutine_t*> &routinue_list) {
  auto entry = rd_wait_list_.find(uid);
  if( rd_wait_list_.end() == entry ) return 0;
  for(auto itr = entry->second.begin(); itr != entry->second.end(); ++itr)
    routinue_list.push_back((*itr)->routinue_);
  rd_wait_list_.erase(entry);
  return 0;
}

void ThreadLockTable::timeout_trans() {
  LOG_DEBUG("timeout trans to avoid deadlock");
  int64_t bound = common::local_time() - ILock::MAX_WAIT_TIME;
  timeout_trans(wr_wait_list_, bound);
  timeout_trans(rd_wait_list_, bound);
}

void ThreadLockTable::timeout_trans(WaitList &wait_list, const int64_t bound) {

  auto list_cur  = wait_list.begin();
  auto list_next = wait_list.begin();

  while (wait_list.end() != list_next ) {
    list_cur = list_next++; 
    auto entry_cur = list_cur->second.begin();
    auto entry_next = list_cur->second.begin();
    while( list_cur->second.end() != entry_next ) {
      entry_cur = entry_next++; 
      TransCtx *ctx = *entry_cur;
      if( ctx->wait_start_time_ > bound ) break;

      list_cur->second.erase(entry_cur);
      //LOG_WARN("timeout ctx %lld", ctx->trans_id_);
      ctx->trans_state_ = -1;
      stCoRoutine_t *routine = ctx->routinue_;
      co_resume(routine);
      if( routine->cEnd )
        dbserver::DbWorker::free_routine(routine);
    }
    if( list_cur->second.empty() )
      wait_list.erase(list_cur);
  }
}

void ILock::init(const int64_t thread_num) {
  thread_num_ = thread_num;
}

void ILock::init_thread_context(Worker *wrk) {
  host_ = wrk;
  thread_index_ = wrk->get_thread_index();
  wrk->set_timed_task_interval(MAX_WAIT_TIME);
  lock_table_ = new ThreadLockTable();
}

RC ILock::acquire(const Lock_type type, TransCtx *ctx, Header *lock_header) {
  if( L_READ == type ) 
    return rd_lock(ctx, lock_header);
  else
    return wr_lock(ctx, lock_header);
}

RC ILock::wr_lock(TransCtx *ctx, Header *lock_header) {

  uint64_t trans_id = ctx->trans_id_;
  uint64_t lock_state = lock_header->lock_;
  uint64_t new_state = (MASK_EXCLUSIVE | (trans_id & MASK_OWNER));
  LOG_DEBUG("ilock: %lld acquire write lock, %p", trans_id, lock_header);

  if( EXCLUSIVE_MODE(lock_state) && OWN(lock_state, trans_id) )
    return RC_OK;

  const uint64_t expectation = ctx->has_rd_lock(lock_header) ? 1 : 0;

  while ( expectation == lock_state ) {
    if( lock_state == ATOMIC_CAS(&lock_header->lock_, lock_state, new_state) ) {
      ctx->add_wr_lock(lock_header);
      return RC_OK;
    }
    asm("pause\n");
    lock_state = lock_header->lock_;
  }
 
  yield_for_write_lock(ctx, lock_header);

  if( -1 == ctx->trans_state_ )
    return RC_ABORT;
  
  if( MASK_CANDIDATE == ATOMIC_CAS(&lock_header->lock_, MASK_CANDIDATE, new_state) ) {
    ctx->add_wr_lock(lock_header);
  }
  else { //should never reach here
    LOG_ERROR("lock state: %x", lock_header->lock_);
    assert(0);
  }
  return RC_OK;
}

void ILock::yield_for_write_lock(TransCtx *ctx, Header *lock_header) {
  LOG_DEBUG("yield trans %lld for write lock %p", ctx->trans_id_, lock_header);
  set_wait_bit(lock_header->wr_wait_bits_);
  __sync_synchronize();

  //TODO try to acquire the lock again, in case any other 
  //just release the lock before we set up the wait flag

  ctx->routinue_ = co_self();
  ctx->wait_start_time_ = common::local_time();
  lock_table_->add_write_waiter((uint64_t)lock_header, ctx);
  co_yield(ctx->routinue_);

  LOG_DEBUG("resume trans %lld", ctx->trans_id_);
}

RC ILock::rd_lock(TransCtx *ctx, Header *lock_header) {
  uint64_t lock_state = lock_header->lock_, new_state;
  uint64_t trans_id   = ctx->trans_id_;

  if( (EXCLUSIVE_MODE(lock_state) && OWN(lock_state, trans_id)) 
      || ctx->has_rd_lock(lock_header) )
    return RC_OK;

  while( !EXCLUSIVE_MODE(lock_state) && lock_state != MASK_CANDIDATE ) {
    new_state = lock_state + 1;
    assert(new_state < MASK_EXCLUSIVE);

    if( lock_state == ATOMIC_CAS(&lock_header->lock_, lock_state, new_state) ) {
      ctx->add_rd_lock(lock_header);
      return RC_OK;
    }
    asm("pause\n");
    lock_state = lock_header->lock_;
  }

  yield_for_read_lock(ctx, lock_header);
  if( -1 == ctx->trans_state_ )
    return RC_ABORT;

  lock_state = lock_header->lock_;
  while( !EXCLUSIVE_MODE(lock_state) ) {
    new_state = lock_state + 1;
 
    if( lock_state == ATOMIC_CAS(&lock_header->lock_, lock_state, new_state) ) {
      ctx->add_rd_lock(lock_header);
      return RC_OK;
    }
    asm("pause\n");
    lock_state = lock_header->lock_;
  }
  //should never reach here
  assert(0);
  return RC_ABORT;
}

void ILock::yield_for_read_lock(TransCtx *ctx, Header *lock_header) {
  LOG_DEBUG("yield trans %lld for read lock %p", ctx->trans_id_, lock_header);
  set_wait_bit(lock_header->rd_wait_bits_);
  __sync_synchronize();

  //TODO try to acquire the lock once more

  ctx->routinue_ = co_self();
  ctx->wait_start_time_ = common::local_time();
  lock_table_->add_read_waiter((uint64_t)lock_header, ctx);
  co_yield(ctx->routinue_);
  LOG_DEBUG("resume trans %lld", ctx->trans_id_);
}

RC ILock::release(const Lock_type type, TransCtx *trans_ctx, Header *header) {
  LOG_DEBUG("trans %lld release %s lock %p", trans_ctx == NULL ? 0 : trans_ctx->trans_id_,
      type == L_READ ? "read" : "write", header);
  if( type == L_READ )
    release_rd_lock(trans_ctx->trans_id_, header);
  else
    release_wr_lock(trans_ctx->trans_id_, header);
  return RC_OK;
}

void ILock::release_rd_lock(const uint64_t trans_id, Header *header) {
  uint64_t lock_state = header->lock_, new_state;
  uint64_t rd_wait_state = header->rd_wait_bits_;
  uint64_t wr_wait_state = header->wr_wait_bits_;
  assert(0 != lock_state && MASK_EXCLUSIVE > lock_state); 
  while(true) {
    lock_state = header->lock_;
    new_state = lock_state - 1; //decrease the reference
    assert(new_state >= 0);
    if( new_state == 0 && (0 != rd_wait_state || 0 != wr_wait_state) )
      new_state = MASK_CANDIDATE;
    if( lock_state == ATOMIC_CAS(&header->lock_, lock_state, new_state) )
      break;
    asm("pause\n");
  }

  if( MASK_CANDIDATE == new_state ) {
    if( 0 != wr_wait_state )
      send_write_lock(header, wr_wait_state);
    else if( 0 != rd_wait_state ) 
      send_read_locks(header, rd_wait_state);
  }
}

void ILock::release_wr_lock(const uint64_t trans_id, Header *header) {
  uint64_t lock_state = header->lock_, new_state = 0;
  uint64_t rd_wait_state = header->rd_wait_bits_;
  uint64_t wr_wait_state = header->wr_wait_bits_;
  assert(EXCLUSIVE_MODE(lock_state) && OWN(lock_state, trans_id));

  if( 0 != wr_wait_state || 0 != rd_wait_state )
    new_state = MASK_CANDIDATE;
  assert(lock_state == ATOMIC_CAS(&header->lock_, lock_state, new_state));

  if( MASK_CANDIDATE == new_state ) {
    if( 0 != rd_wait_state ) 
      send_read_locks(header, header->rd_wait_bits_);
    else if( 0 != wr_wait_state )
      send_write_lock(header, header->wr_wait_bits_);
  }
}

void ILock::send_read_locks(Header *header, const uint64_t wait_bits) {
  assert(MASK_CANDIDATE == ATOMIC_CAS(&header->lock_, MASK_CANDIDATE, __builtin_popcount(wait_bits)));
  __sync_synchronize();
  for(int64_t i = 0; i < thread_num_; ++i) {
    if( 0 != (wait_bits & (1 << i)) ) {
      GrantLockPacket *lock_pkt = new GrantLockPacket((uint64_t) header, L_READ);
      host_->push(lock_pkt, i, PRIORITY_HIGH);
    }
  }
}

void ILock::send_write_lock(Header *header, const uint64_t wait_bits) {
  int64_t next_thread_index;
  __sync_synchronize();
  for(int64_t i = 1; i <= thread_num_; ++i) {
    next_thread_index = (i + thread_index_) % thread_num_;
    if( 0 != (wait_bits & (1 << next_thread_index)) ) {
      GrantLockPacket *lock_pkt = new GrantLockPacket((uint64_t) header, L_WRITE);
      host_->push(lock_pkt, next_thread_index, PRIORITY_HIGH);
      break;
    }
  }
}

//resume a blocked transaction
void ILock::wakeup_candidate(const Lock_type type, Header *lock_header) {
  int clear, num = 0;
  if( type == L_READ ) {
    vector<stCoRoutine_t *> read_routines;
    if( 0 == lock_table_->get_read_waiter((uint64_t)lock_header, read_routines) )
      clear_wait_bit(lock_header->rd_wait_bits_);
    for(auto iter = read_routines.begin(); iter != read_routines.end(); ++iter) {
      co_resume(*iter);
      if( (*iter)->cEnd )
        dbserver::DbWorker::free_routine(*iter);
    }
    release_rd_lock(0, lock_header); //releae fake read lock
  }
  else {
    stCoRoutine_t * write_routine = NULL;
    if( 0 == lock_table_->get_write_waiter((uint64_t)lock_header, write_routine) )
      clear_wait_bit(lock_header->wr_wait_bits_);
    if( NULL != write_routine ) {
      co_resume(write_routine);
      if( write_routine->cEnd )
        dbserver::DbWorker::free_routine(write_routine);
    }
    else
      release_wr_lock(0, lock_header); //release unused write lock
  }
}

//help functions
void ILock::clear_wait_bit(volatile uint64_t &wait_bits) {
  uint64_t old_state = wait_bits, new_state;
  assert(0 != (old_state & (1 << thread_index_)));
  do {
    old_state = wait_bits;
    new_state = old_state ^ (1 << thread_index_);
  } while( old_state != ATOMIC_CAS(&wait_bits, old_state, new_state) );
}

void ILock::set_wait_bit(volatile uint64_t &wait_bits) {
  uint64_t old_state = wait_bits, new_state;
  if( 0 != (old_state & (1 << thread_index_)) ) return;
  do {
    old_state = wait_bits;
    new_state = old_state | (1 << thread_index_); 
  } while( old_state != ATOMIC_CAS(&wait_bits, old_state, new_state) );
}

void ILock::timeout_trans() {
  lock_table_->timeout_trans();
}
