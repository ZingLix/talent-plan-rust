use super::ThreadPool;
use crate::{KvsError, KvsErrorType, Result};

/// rayon::ThreadPool 针对 ThreadPool trait 的封装
pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    /// 创建线程数量为 thread_num 的线程池
    ///
    /// 0 代表使用运行 CPU 的核心数量。
    fn new(thread_num: usize) -> Result<Self> {
        let mut thread_count = thread_num;
        if thread_count == 0 {
            thread_count = num_cpus::get();
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .build()
            .map_err(|_| KvsError::from(KvsErrorType::Other))?;
        Ok(RayonThreadPool(pool))
    }

    /// 提交一个任务 job
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.spawn(job)
    }
}
