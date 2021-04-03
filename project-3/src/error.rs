use failure::Context;
use failure::{Backtrace, Fail};
use std::fmt::{self, Display};

/// 对 KvsErrorType 的一层封装
#[derive(Debug)]
pub struct KvsError {
    inner: Context<KvsErrorType>,
}

/// Kvs 系统中可能出现的错误类型
#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum KvsErrorType {
    /// IO 错误（文件、网络）
    #[fail(display = "IOError")]
    IOError,
    /// serde 解析错误
    #[fail(display = "SerdeError")]
    SerdeError,
    /// 数据库未知操作
    #[fail(display = "UnknownOperation")]
    UnknownOperation,
    /// Key 不存在
    #[fail(display = "KeyNotFound")]
    KeyNotFound,
    /// sled 错误
    #[fail(display = "SledError")]
    SledError,
    /// 其他错误
    #[fail(display = "Other")]
    Other,
}

impl Fail for KvsError {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for KvsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl KvsError {
    /// 获取错误类型
    pub fn kind(&self) -> KvsErrorType {
        *(self.inner.get_context())
    }
}

impl From<KvsErrorType> for KvsError {
    fn from(kind: KvsErrorType) -> KvsError {
        KvsError {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<KvsErrorType>> for KvsError {
    fn from(inner: Context<KvsErrorType>) -> KvsError {
        KvsError { inner: inner }
    }
}

impl From<std::io::Error> for KvsError {
    fn from(_: std::io::Error) -> KvsError {
        KvsErrorType::IOError.into()
    }
}

impl From<sled::Error> for KvsError {
    fn from(_: sled::Error) -> KvsError {
        KvsErrorType::SledError.into()
        //KvsError::from(KvsErrorType::SledError { err })
    }
}

/// 别名，用于简化使用 KvsError 的 Result
pub type Result<T> = std::result::Result<T, KvsError>;
