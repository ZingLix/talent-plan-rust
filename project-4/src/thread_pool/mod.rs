//! 线程池 模块
//! 所有线程池均实现 ThreadPool trait

use super::Result;
mod naive_thread_pool;
mod rayon_thread_pool;
mod shared_thread_pool;

pub use self::naive_thread_pool::NaiveThreadPool;
pub use self::rayon_thread_pool::RayonThreadPool;
pub use self::shared_thread_pool::SharedQueueThreadPool;

/// 定义了线程池的 trait
pub trait ThreadPool {
    /// 创建一个线程数量为 thread_num 的线程池
    fn new(thread_count: usize) -> Result<Self>
    where
        Self: Sized;

    /// 像线程池中提交一个任务 job
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}
