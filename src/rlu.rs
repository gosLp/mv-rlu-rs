#![allow(dead_code, unused_variables)]

use std::fmt::Debug;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::thread;
use std::usize;
use std::cell::UnsafeCell;

const RLU_MAX_LOG_SIZE: usize = 128;
const RLU_MAX_THREADS: usize = 32;
const RLU_MAX_FREE_NODES: usize = 100;

pub struct ObjOriginal<T> {
  copy: AtomicPtr<ObjCopy<T>>,
  data: T,
}

#[derive(Clone)]
pub struct ObjCopy<T> {
  thread_id: usize,
  original: RluObject<T>,
  data: T,
}

#[derive(Debug)]
pub struct RluObject<T>(pub(crate) *mut ObjOriginal<T>);

impl<T> Clone for RluObject<T> {
  fn clone(&self) -> Self {
    *self
  }
}
impl<T> Copy for RluObject<T> {}

impl<T> RluObject<T> {
  fn deref(&self) -> &ObjOriginal<T> {
    unsafe { &*self.0 }
  }

  fn deref_mut(&mut self) -> &mut ObjOriginal<T> {
    unsafe { &mut *self.0 }
  }

  pub fn null() -> RluObject<T> {
    RluObject(ptr::null_mut())
  }
}

struct WriteLog<T> {
  entries: [MaybeUninit<ObjCopy<T>>; RLU_MAX_LOG_SIZE],
  num_entries: usize,
}

impl<T> WriteLog<T> {
  pub fn new() -> Self {
    Self {
      entries: unsafe { MaybeUninit::uninit().assume_init() },
      num_entries: 0,
    }
  }
  
}

impl<T> Drop for WriteLog<T> {
  fn drop(&mut self) {
      for i in 0..self.num_entries {
          unsafe {
              self.entries.get_unchecked_mut(i).assume_init_drop();
          }
      }
  }
}


pub struct RluThread<T> {
  logs: [WriteLog<T>; 2],
  current_log: usize,
  is_writer: bool,
  write_clock: usize,
  local_clock: AtomicUsize,
  run_counter: AtomicUsize,
  thread_id: usize,
  global: *const Rlu<T>,
  free_list: [RluObject<T>; RLU_MAX_FREE_NODES],
  num_free: usize,
}

pub struct Rlu<T> {
  global_clock: AtomicUsize,
  threads: [UnsafeCell<RluThread<T>>; RLU_MAX_THREADS],
  num_threads: AtomicUsize,
}

unsafe impl<T> Sync for Rlu<T> where T: Sync { }

unsafe impl<T> Send for RluObject<T> {}
unsafe impl<T> Sync for RluObject<T> {}

unsafe impl<T> Send for RluThread<T> {}
unsafe impl<T> Sync for RluThread<T> {}

pub struct RluSession<'a, T: RluBounds> {
  t: &'a mut RluThread<T>,
  abort: bool,
}

pub trait RluBounds: Clone + Debug {}
impl<T: Clone + Debug> RluBounds for T {}

impl<T> WriteLog<T> {
  fn next_entry(&mut self, thread_id: usize, data: T, original: RluObject<T>) -> &mut ObjCopy<T> {
    let i = self.num_entries;
    self.num_entries += 1;

    if cfg!(debug_assertions) {
      assert!(self.num_entries < RLU_MAX_LOG_SIZE)
    }

    unsafe { 
      let entry = self.entries.get_unchecked_mut(i);
      entry.write(ObjCopy{
        thread_id,
        original,
        data,
      });
      entry.assume_init_mut()
    }
  }
}


impl<T: RluBounds> Rlu<T> {
  pub fn new() -> Rlu<T> {
    let mut threads: [MaybeUninit<UnsafeCell<RluThread<T>>>; RLU_MAX_THREADS] =
      unsafe { MaybeUninit::uninit().assume_init() };
    for elem in &mut threads {
      elem.write(UnsafeCell::new(RluThread::new())); // Wrap each cell in unsafe thread
    }
    let threads: [UnsafeCell<RluThread<T>>; RLU_MAX_THREADS] =
      unsafe { std::mem::MaybeUninit::array_assume_init(threads) };
    Rlu {
      global_clock: AtomicUsize::new(0),
      num_threads: AtomicUsize::new(0),
      threads,
    }
  }

  pub fn thread(&mut self) -> &mut RluThread<T> {
    let thread_id = self.num_threads.fetch_add(1, Ordering::SeqCst);
    let thread: *mut RluThread<T> = unsafe { &mut *self.threads[thread_id].get()};
    let thread: &mut RluThread<T> = unsafe { &mut *thread };
    *thread = RluThread::new();
    thread.thread_id = thread_id;
    thread.global = self as *const Rlu<T>;
    thread
  }

  fn get_thread(&self, index: usize) -> *mut RluThread<T> {
    // (unsafe { self.threads.get_unchecked(index) }) as *const RluThread<T>
    //   as *mut RluThread<T>
    unsafe { self.threads.get_unchecked(index).get() }
  }

  pub fn alloc(&self, data: T) -> RluObject<T> {
    RluObject(Box::into_raw(Box::new(ObjOriginal {
      copy: AtomicPtr::new(ptr::null_mut()),
      data,
    })))
  }
}

macro_rules! log {
  ($self:expr, $e:expr) => {
    if cfg!(debug_assertions) {
      let s: String = $e.into();
      println!("Thread {}: {}", $self.thread_id, s);
    }
  };
}

impl<'a, T: RluBounds> RluSession<'a, T> {
  pub fn read_lock(&mut self, obj: RluObject<T>) -> *const T {
    log!(self.t, "dereference");
    let global = unsafe { &*self.t.global };
    let orig = obj.deref();
    match unsafe { orig.copy.load(Ordering::SeqCst).as_ref() } {
      None => &orig.data,
      Some(copy) => {
        if self.t.thread_id == copy.thread_id {
          log!(
            self.t,
            format!("dereference self copy {:?} ({:p})", copy.data, &copy.data)
          );
          &copy.data
        } else {
          let thread = unsafe { &*global.get_thread(copy.thread_id) };
          if thread.write_clock <= self.t.local_clock.load(Ordering::SeqCst) {
            log!(self.t,
                 format!("dereference other copy {:?} ({:p}), write clock {}, local clock {}", copy.data, &copy.data, thread.write_clock, self.t.local_clock.load(Ordering::SeqCst)));
            &copy.data
          } else {
            log!(
              self.t,
              format!(
                "dereferencing original {:?} ({:p})",
                orig.data, &orig.data
              )
            );
            &orig.data
          }
        }
      }
    }
  }

  pub fn write_lock(&mut self, mut obj: RluObject<T>) -> Option<*mut T> {
    log!(self.t, format!("try_lock"));
    let global = unsafe { &*self.t.global };
    self.t.is_writer = true;

    if let Some(copy) =
      unsafe { obj.deref_mut().copy.load(Ordering::SeqCst).as_mut() }
    {
      if self.t.thread_id == copy.thread_id {
        log!(
          self.t,
          format!("locked existing copy {:?} ({:p})", copy.data, &copy.data)
        );
        return Some(&mut copy.data as *mut T);
      } else {
        return None;
      }
    }

    let data_clone = obj.deref().data.clone();
    let active_log = &mut self.t.logs[self.t.current_log];
    let copy = active_log.next_entry(self.t.thread_id, data_clone, obj);
    copy.thread_id = self.t.thread_id;
    copy.data = obj.deref().data.clone();
    copy.original = obj;
    let prev_ptr = obj.deref_mut().copy.compare_exchange(
      ptr::null_mut(),
      copy as *mut _,
      Ordering::SeqCst,
      Ordering::SeqCst,
    );
    if prev_ptr.is_err() {
      active_log.num_entries -= 1;
      unsafe {
        active_log.entries.get_unchecked_mut(active_log.num_entries).assume_init_drop();
      }
      return None;
    }

    log!(
      self.t,
      format!("locked new copy {:?} ({:p})", copy.data, &copy.data)
    );

    Some(&mut copy.data as *mut T)
  }

  pub fn abort(mut self) {
    self.abort = true;
  }
}

impl<'a, T: RluBounds> Drop for RluSession<'a, T> {
  fn drop(&mut self) {
    log!(self.t, "drop");
    if self.abort {
      self.t.abort();
    } else {
      self.t.unlock();
    }
  }
}

impl<T: RluBounds> RluThread<T> {
  fn new() -> RluThread<T> {
    let mut logs: [MaybeUninit<WriteLog<T>>; 2] =
      unsafe { MaybeUninit::uninit().assume_init() };
    for elm in &mut logs {
      elm.write(WriteLog::new());
    }
    let logs: [WriteLog<T>; 2] = unsafe { std::mem::MaybeUninit::array_assume_init(logs) };
    let mut free_list: [MaybeUninit<RluObject<T>>; RLU_MAX_FREE_NODES] =
      unsafe { MaybeUninit::uninit().assume_init() };
    for elm in &mut free_list {
      elm.write(RluObject::null());
    }
    let free_list: [RluObject<T>; RLU_MAX_FREE_NODES] =
      unsafe { std::mem::transmute(free_list) };
    let mut thread = RluThread {
      //logs: unsafe { mem::uninitialized() },
      logs,
      current_log: 0,
      is_writer: false,
      write_clock: usize::MAX,
      local_clock: AtomicUsize::new(0),
      run_counter: AtomicUsize::new(0),
      thread_id: 0,
      global: ptr::null(),
      num_free: 0,
      free_list,
    };

    for i in 0..2 {
      thread.logs[i].num_entries = 0;
    }

    thread
  }

  pub fn session<'a>(&'a mut self) -> RluSession<'a, T> {
    log!(self, "lock");
    let global = unsafe { &*self.global };
    let cntr = self.run_counter.fetch_add(1, Ordering::SeqCst);
    if cfg!(debug_assertions) {
      assert!(cntr % 2 == 0);
    }

    self
      .local_clock
      .store(global.global_clock.load(Ordering::SeqCst), Ordering::SeqCst);
    log!(
      self,
      format!(
        "lock with local clock {}",
        self.local_clock.load(Ordering::SeqCst)
      )
    );
    self.is_writer = false;
    RluSession {
      t: self,
      abort: false,
    }
  }

  pub fn free(&mut self, obj: RluObject<T>) {
    let free_id = self.num_free;
    self.num_free += 1;
    self.free_list[free_id] = obj;
  }

  fn process_free(&mut self) {
    for i in 0..self.num_free {
      unsafe { 
        drop(Box::from_raw(self.free_list[i].0));
      };
    }

    self.num_free = 0;
  }

  fn commit_write_log(&mut self) {
    let global = unsafe { &*self.global };
    self.write_clock = global.global_clock.fetch_add(1, Ordering::SeqCst) + 1;
    log!(self, format!("global clock: {}", self.write_clock));
    self.synchronize();
    self.writeback_logs();
    self.unlock_write_log();
    self.write_clock = usize::MAX;
    self.swap_logs();
    self.process_free();
  }

  fn unlock(&mut self) {
    log!(self, "unlock");
    let cntr = self.run_counter.fetch_add(1, Ordering::SeqCst);
    if cfg!(debug_assertions) {
      assert!(cntr % 2 == 1);
    }

    if self.is_writer {
      self.commit_write_log();
    }
  }

  fn writeback_logs(&mut self) {
    log!(self, "writeback_logs");
    let active_log = &mut self.logs[self.current_log];
    for i in 0..active_log.num_entries {
      let copy = unsafe { active_log.entries.get_unchecked_mut(i).assume_init_mut() };
      log!(self, format!("copy {:?} ({:p})", copy.data, &copy.data));
      let orig = copy.original.deref_mut();
      orig.data = copy.data.clone();
    }
  }

  fn unlock_write_log(&mut self) {
    log!(self, "unlock_write_log");
    let active_log = &mut self.logs[self.current_log];
    for i in 0..active_log.num_entries {
      let copy: &mut ObjCopy<T> = unsafe { active_log.entries.get_unchecked_mut(i).assume_init_mut() };
      let orig = copy.original.deref_mut();
      orig.copy.store(ptr::null_mut(), Ordering::SeqCst);
    }
    // Drop the initialized entries to avoid memory leaks
    for i in 0..active_log.num_entries {
      unsafe {
          active_log.entries.get_unchecked_mut(i).assume_init_drop();
      }
    }
    active_log.num_entries = 0;
  }

  fn swap_logs(&mut self) {
    log!(self, "swap_logs");
    self.current_log = (self.current_log + 1) % 2;
    let active_log = &mut self.logs[self.current_log];
    active_log.num_entries = 0;
  }

  fn synchronize(&mut self) {
    log!(self, "synchronize");

    let global = unsafe { &*self.global };
    let num_threads = global.num_threads.load(Ordering::SeqCst);
    let run_counts: Vec<usize> = (0..num_threads)
      .map(|i| {
        unsafe {
          let thread = &*global.threads[i].get(); // ACcess the inner RluThread
          thread.run_counter.load(Ordering::SeqCst)
        }
      })
      .collect();

    for i in 0..num_threads {
      if i == self.thread_id {
        continue;
      }

      // let thread = &global.threads[i];
      unsafe {
        let thread = &*global.threads[i].get(); //Access the inner RluThread
        loop {
          log!(self, format!(
            "wait on thread {}: rc {}, counter {}, write clock {}, local clock {}",
            i,
            run_counts[i],
            thread.run_counter.load(Ordering::SeqCst),
            self.write_clock,
            thread.local_clock.load(Ordering::SeqCst)
          ));

          if run_counts[i] % 2 == 0
              || thread.run_counter.load(Ordering::SeqCst) != run_counts[i]
              || self.write_clock <= thread.local_clock.load(Ordering::SeqCst)
          {
            break;
          }
          thread::yield_now();
        }
      }
    }
  }

  fn abort(&mut self) {
    log!(self, "abort");
    let cntr = self.run_counter.fetch_add(1, Ordering::SeqCst);
    if cfg!(debug_assertions) {
      assert!(cntr % 2 == 1);
    }

    if self.is_writer {
      self.unlock_write_log();
    }
  }
}
