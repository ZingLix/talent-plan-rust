use super::ThreadPool;
use crate::Result;
use std::thread;

/// 符合 ThreadPool trait 的假线程池
///
/// 仅仅是每个新任务都创建一个新线程去执行
pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    /// 创建一个 NaiveThreadPool
    ///
    /// _thread_count 无用
    fn new(_thread_count: usize) -> Result<Self> {
        Ok(NaiveThreadPool {})
    }

    /// 提交一个任务 job
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
