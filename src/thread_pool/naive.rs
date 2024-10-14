// use super::ThreadPool;
// use crate::Result;
// use std::thread;

// /// It is actually not a thread pool. It spawns a new thread every time
// /// the `spawn` method is called.
// pub struct NaiveThreadPool;

// impl ThreadPool for NaiveThreadPool {
//     fn new(_threads: u32) -> Result<Self> {
//         Ok(NaiveThreadPool)
//     }

//     fn spawn<F>(&self, job: F)
//     where
//         F: FnOnce() + Send + 'static,
//     {
//         thread::spawn(job);
//     }
// }

use super::ThreadPool;
use crate::Result;
use std::thread;

/// the name is xxxPool, but it is not a "Pool"
pub struct NaiveThreadPool;

/// It is actually not a thread pool. It spawns a new thread every time
/// the `spawn` method is called.
impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        Ok(NaiveThreadPool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
