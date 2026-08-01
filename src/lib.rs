use axum::{
    body::Body,
    extract::{Json, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use bytes::Bytes;
use dashmap::DashMap;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

lazy_static! {
    static ref RANGE_RE: Regex = Regex::new(r"bytes=(\d+)-(\d*)").unwrap();
    static ref CONTENT_RANGE_RE: Regex = Regex::new(r"bytes \d+-\d+/(\d+)").unwrap();
    static ref HTTP_CLIENT: reqwest::Client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(32)
        .connect_timeout(Duration::from_secs(10))
        .tcp_keepalive(Duration::from_secs(30))
        .build()
        .unwrap();
}

const PART_SIZE: i64 = 1024 * 1024;

#[derive(Clone)]
struct AppState {
    url_map: Arc<DashMap<String, String>>,
    header_map: Arc<DashMap<String, HashMap<String, String>>>,
    cancel_map: Arc<DashMap<String, Arc<AtomicBool>>>,
    config: Arc<Mutex<Config>>,
}

struct Config {
    port: u16,
    thread_num: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct Request {
    url: String,
    headers: HashMap<String, String>,
    key: String,
}

async fn find_available_port(mut port: u16) -> u16 {
    loop {
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        if TcpListener::bind(addr).await.is_ok() {
            return port;
        }
        println!("端口 {} 已被占用，尝试下一个端口...", port);
        port += 1;
    }
}

#[no_mangle]
pub unsafe extern "C" fn Java_com_github_catvod_spider_LuProxyNative_StartServer() {
   

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let state = AppState {
            url_map: Arc::new(DashMap::new()),
            header_map: Arc::new(DashMap::new()),
            cancel_map: Arc::new(DashMap::new()),
            config: Arc::new(Mutex::new(Config {
                port: 12345,
                thread_num: 16,
            })),
        };

        let port = {
            let config = state.config.lock().await;
            find_available_port(config.port).await
        };

        {
            let mut config = state.config.lock().await;
            config.port = port;
        }

        println!("启动服务 on {}", port);

        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let listener = TcpListener::bind(addr).await.unwrap();

        let app = Router::new()
            .route("/", get(root_handler))
            .route("/buildUrl", post(build_url_handler))
            .route("/proxy", get(proxy_handler))
            .with_state(state);

        axum::serve(listener, app).await.unwrap();
    });
}

async fn root_handler() -> &'static str {
    "ser200"
}

async fn build_url_handler(
    State(state): State<AppState>,
    Json(req): Json<Request>,
) -> impl IntoResponse {
    state.url_map.clear();
    state.header_map.clear();
    state.url_map.insert(req.key.clone(), req.url);
    state.header_map.insert(req.key, req.headers);
    println!("配置已更新: key={}", state.url_map.len());
    StatusCode::OK
}

async fn proxy_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let key = params.get("key").cloned().unwrap_or_default();
    let threads_param = params.get("threads").cloned().unwrap_or_default();

    // 取消同 key 的旧下载任务
    if let Some(cancel_flag) = state.cancel_map.get(&key) {
        cancel_flag.store(true, Ordering::Relaxed);
        println!("已取消 key={} 的旧下载任务", key);
    }

    let cancel_flag = Arc::new(AtomicBool::new(false));
    state.cancel_map.insert(key.clone(), cancel_flag.clone());

    // 设置线程数
    let thread_num = {
        let mut config = state.config.lock().await;
        if let Ok(t) = threads_param.parse::<usize>() {
            config.thread_num = t;
        }
        println!("启动线程数{}", config.thread_num);
        config.thread_num
    };

    let url = state
        .url_map
        .get(&key)
        .map(|r| r.value().clone())
        .unwrap_or_default();
    let headers_map = state
        .header_map
        .get(&key)
        .map(|r| r.value().clone())
        .unwrap_or_default();

    println!("URL: {}", url);
    println!("headers: {:?}", headers_map);

    let range_header = headers
        .get("range")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "bytes=0-".to_string());

    let mut new_headers = headers_map.clone();
    new_headers.insert("Range".to_string(), range_header.clone());

    let (start_point, end_point) = parse_range_point(&range_header);
    println!("解析 Range 点: start={}, end={}", start_point, end_point);

    let info = get_info(&url, &new_headers).await.unwrap_or_default();
    let content_length = get_content_length(&info);
    println!("内容长度: {}, info: {:?}", content_length, info);

    let mut final_end_point = end_point;
    if end_point == -1 {
        final_end_point = content_length - 1;
    }
    println!("最终下载范围: start={}, end={}", start_point, final_end_point);

    if start_point > final_end_point || content_length == 0 {
        println!("无效的下载范围，跳过");
        return (StatusCode::INTERNAL_SERVER_ERROR, "Invalid range").into_response();
    }

    let content_length_value = final_end_point - start_point + 1;
    let content_range_value = format!(
        "bytes {}-{}/{}",
        start_point, final_end_point, content_length
    );

    let content_type = info
        .get("content-type")
        .cloned()
        .unwrap_or_else(|| "video/mp4".to_string());

    let mut response = Response::new(Body::from_stream(stream_proxy(
        url,
        new_headers,
        thread_num,
        start_point,
        final_end_point,
        content_length,
        cancel_flag,
    )));
    *response.status_mut() = StatusCode::PARTIAL_CONTENT;
    response
        .headers_mut()
        .insert("Connection", HeaderValue::from_static("keep-alive"));
    response
        .headers_mut()
        .insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));
    response.headers_mut().insert(
        "Content-Type",
        HeaderValue::from_str(&content_type).unwrap_or_else(|_| HeaderValue::from_static("video/mp4")),
    );
    response.headers_mut().insert(
        "Content-Length",
        HeaderValue::from_str(&content_length_value.to_string()).unwrap_or_else(|_| HeaderValue::from_static("0")),
    );
    response.headers_mut().insert(
        "Content-Range",
        HeaderValue::from_str(&content_range_value).unwrap_or_else(|_| HeaderValue::from_static("bytes */*")),
    );

    response
}

fn stream_proxy(
    url: String,
    headers: HashMap<String, String>,
    thread_num: usize,
    start_point: i64,
    final_end_point: i64,
    content_length: i64,
    cancel_flag: Arc<AtomicBool>,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> {
    async_stream::try_stream! {
        let mut current_start = start_point;
        while current_start <= final_end_point {
            if cancel_flag.load(Ordering::Relaxed) {
                println!("检测到取消标志，准备退出下载循环");
                break;
            }

            let mut tasks = Vec::new();
            for _ in 0..thread_num {
                if current_start > final_end_point { break; }

                let chunk_start = current_start;
                let chunk_end = std::cmp::min(current_start + PART_SIZE - 1, final_end_point);

                let url_clone = url.clone();
                let headers_clone = headers.clone();
                let cancel_clone = cancel_flag.clone();

                let task = tokio::spawn(async move {
                    if cancel_clone.load(Ordering::Relaxed) {
                        println!("任务被取消，跳过 chunk");
                        return None;
                    }
                    match get_video_stream(chunk_start, chunk_end, content_length, &url_clone, &headers_clone).await {
                        Some(data) => {
                            if cancel_clone.load(Ordering::Relaxed) {
                                println!("数据获取后任务被取消");
                                return None;
                            }
                            let mut final_data = data;
                            // 只有第一个分片才需要检测/移除恶意前缀
                            if chunk_start == 0 {
                                let offset = detect_malicious_prefix(&final_data);
                                if offset > 0 {
                                    println!("发现并移除恶意前缀: offset={}, chunk={}-{}", offset, chunk_start, chunk_end);
                                    final_data = final_data.slice(offset..);
                                }
                            }
                            println!("成功获取数据块: {} bytes, chunk={}-{}", final_data.len(), chunk_start, chunk_end);
                            Some(final_data)
                        }
                        None => {
                            println!("get_video_stream 返回 None: chunk={}-{}", chunk_start, chunk_end);
                            None
                        }
                    }
                });

                tasks.push(task);
                current_start = chunk_end + 1;
            }

            for task in tasks {
                if cancel_flag.load(Ordering::Relaxed) {
                    println!("下载已取消，停止接收数据。");
                    break;
                }
                match task.await {
                    Ok(Some(data)) if !data.is_empty() => {
                        yield data;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        println!("下载任务 join 失败: {}", e);
                    }
                }
            }
        }
        println!("代理下载完成");
    }
}

async fn get_video_stream(
    start: i64,
    end: i64,
    content_length: i64,
    url: &str,
    headers: &HashMap<String, String>,
) -> Option<Bytes> {
    if start > content_length {
        return None;
    }

    println!("开始获取视频片段 {}-{}", start, end);

    let mut req = HTTP_CLIENT
        .get(url)
        .header("Range", format!("bytes={}-{}", start, end));

    for (k, v) in headers {
        if !k.eq_ignore_ascii_case("range") {
            req = req.header(k, v);
        }
    }

    match req.send().await {
        Ok(resp) => resp.bytes().await.ok(),
        Err(e) => {
            println!("请求视频出错: {}", e);
            None
        }
    }
}

fn detect_malicious_prefix(data: &[u8]) -> usize {
    if is_valid_video_header(data) {
        return 0;
    }

    let search_limit = std::cmp::min(256, data.len());
    if search_limit < 16 {
        return 0;
    }

    for offset in 1..(search_limit - 16) {
        if is_valid_video_header(&data[offset..]) {
            println!("发现合法视频头位于偏移 {}，疑似被插入恶意前缀！", offset);
            return offset;
        }
    }
    0
}

fn is_valid_video_header(data: &[u8]) -> bool {
    if data.len() < 8 {
        return false;
    }

    if data[4] == b'f' && data[5] == b't' && data[6] == b'y' && data[7] == b'p' {
        let size = ((data[0] as u64) << 24)
            | ((data[1] as u64) << 16)
            | ((data[2] as u64) << 8)
            | (data[3] as u64);
        if size >= 8 && size <= 0x100000 {
            return true;
        }
    }

    if data.len() >= 4 && &data[0..4] == b"RIFF" {
        return true;
    }

    if data[0] == 0x1A && data[1] == 0x45 && data[2] == 0xDF && data[3] == 0xA3 {
        return true;
    }

    if data.len() >= 4 && &data[0..3] == b"FLV" && data[3] == 0x01 {
        return true;
    }

    false
}

fn parse_range_point(range_header: &str) -> (i64, i64) {
    if let Some(caps) = RANGE_RE.captures(range_header) {
        let start = caps
            .get(1)
            .map_or(0, |m| m.as_str().parse().unwrap_or(0));
        let end = caps.get(2).map_or(-1, |m| {
            if m.as_str().is_empty() {
                -1
            } else {
                m.as_str().parse().unwrap_or(-1)
            }
        });
        return (start, end);
    }
    (0, -1)
}

async fn get_info(
    url: &str,
    headers: &HashMap<String, String>,
) -> Option<HashMap<String, String>> {
    let mut req = HTTP_CLIENT.get(url).header("Range", "bytes=0-0");
    for (k, v) in headers {
        if !k.eq_ignore_ascii_case("range") {
            req = req.header(k, v);
        }
    }
    if let Ok(resp) = req.send().await {
        let mut info = HashMap::new();
        for (k, v) in resp.headers().iter() {
            if let Ok(val_str) = v.to_str() {
                info.insert(k.to_string(), val_str.to_string());
            }
        }
        return Some(info);
    }
    None
}

fn get_content_length(info: &HashMap<String, String>) -> i64 {
    if let Some(content_range) = info.get("content-range") {
        if let Some(caps) = CONTENT_RANGE_RE.captures(content_range) {
            if let Some(total_match) = caps.get(1) {
                return total_match.as_str().parse().unwrap_or(0);
            }
        }
    }
    0
}


