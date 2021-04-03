use super::ThreadPool;
use crate::Result;
use num_cpus;
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

/// 线程池接收的任务类型
type Job = Box<dyn FnOnce() + Send + 'static>;

/// 线程池内部使用的消息
pub enum Message {
    /// 新任务 Job
    NewJob(Job),
    /// 线程终止
    Terminate,
}

/// 使用共享线程的线程池
pub struct SharedQueueThreadPool {
    thread_count: usize,
    sender: mpsc::Sender<Message>,
    thread_map: Arc<Mutex<std::collections::HashMap<usize, JoinHandle<()>>>>,
}

impl ThreadPool for SharedQueueThreadPool {
    /// 创建线程数量为 thread_num 的线程池
    ///
    /// 0 代表使用运行 CPU 的核心数量。
    fn new(thread_num: usize) -> Result<Self>
    where
        Self: Sized,
    {
        let mut thread_count = thread_num;
        if thread_count == 0 {
            thread_count = num_cpus::get();
        }
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let pool = SharedQueueThreadPool {
            thread_count,
            sender,
            thread_map: Arc::new(Mutex::new(HashMap::new())),
        };
        for _ in 0..thread_count {
            let id = pool.thread_map.lock().unwrap().len();
            let rx = receiver.clone();
            let handle = Self::create_thread(id, rx, Arc::clone(&pool.thread_map));
            pool.thread_map.lock().unwrap().insert(id, handle);
        }
        Ok(pool)
    }

    /// 提交一个任务 job
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.send(Message::NewJob(Box::new(job))).unwrap();
    }
}

impl<'b> SharedQueueThreadPool {
    /// 创建一个新的线程
    pub fn create_thread(
        id: usize,
        rx: Arc<Mutex<mpsc::Receiver<Message>>>,
        map: Arc<Mutex<std::collections::HashMap<usize, JoinHandle<()>>>>,
    ) -> JoinHandle<()> {
        thread::spawn(move || worker_thread(id, rx, map))
    }
}

impl Drop for SharedQueueThreadPool {
    /// 实现线程池的优雅终止
    ///
    /// 会执行完队列中所有的剩余任务
    fn drop(&mut self) {
        for _ in 0..self.thread_count {
            self.sender.send(Message::Terminate).unwrap();
        }
        while self.thread_map.lock().unwrap().len() != 0 {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
}

/// 创建一个线程池中的线程，rx 为该线程用于接收消息的对象

/// 工作进程
struct Worker {
    id: usize,
    receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
    thread_map: Arc<Mutex<std::collections::HashMap<usize, JoinHandle<()>>>>,
}

impl Drop for Worker {
    /// 用于检测线程是否 panick
    ///
    /// 利用 RAII 在 worker 的函数运行结束后调用并进行检查
    ///
    /// 如果 panick 则会创建一个新的继续用于执行任务
    fn drop(&mut self) {
        if thread::panicking() {
            let rx = self.receiver.clone();
            let handle = SharedQueueThreadPool::create_thread(
                self.id.clone(),
                rx,
                Arc::clone(&self.thread_map),
            );
            self.thread_map
                .lock()
                .unwrap()
                .insert(self.id.clone(), handle);
        } else {
            self.thread_map.lock().unwrap().remove(&self.id);
        }
    }
}

/// 线程池 worker 执行的函数
fn worker_thread(
    id: usize,
    rx: Arc<Mutex<mpsc::Receiver<Message>>>,
    thread_map: Arc<Mutex<std::collections::HashMap<usize, JoinHandle<()>>>>,
) {
    // 用于线程 panick 后检查
    let worker = Worker {
        id,
        receiver: rx,
        thread_map,
    };
    loop {
        // 从 receiver 处抢锁从而取得任务
        let msg = worker.receiver.lock().unwrap().recv().unwrap();
        match msg {
            // 新的任务则执行，然后循环继续等待下一个任务
            Message::NewJob(job) => {
                job();
            }
            // 终止则结束循环
            Message::Terminate => {
                break;
            }
        }
    }
}
