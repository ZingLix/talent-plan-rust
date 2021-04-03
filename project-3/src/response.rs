use serde::{Deserialize, Serialize};

/// 响应所使用的结构体
///
/// status 表示响应的状态，0为正常，其他为错误
///
/// msg 用于携带可选的消息
#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    pub status: i32,
    pub msg: Option<String>,
}

impl Response {
    /// 带有消息的正常响应
    pub fn ok(msg: String) -> Self {
        Response {
            status: 0,
            msg: Some(msg),
        }
    }

    /// 不带有消息的正常响应
    pub fn ok_without_msg() -> Self {
        Response {
            status: 0,
            msg: None,
        }
    }

    /// 出错时的响应
    pub fn err(msg: String) -> Self {
        Response {
            status: -1,
            msg: Some(msg),
        }
    }
}
