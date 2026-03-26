#![cfg(test)]

use std::collections::VecDeque;
use std::fs::{create_dir_all, remove_dir_all};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct RecordedRequest {
    pub method: String,
    pub path: String,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct MockResponse {
    status: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl MockResponse {
    pub fn json(status: &str, body: &str) -> Self {
        Self {
            status: status.to_string(),
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: body.as_bytes().to_vec(),
        }
    }

    pub fn empty(status: &str) -> Self {
        Self {
            status: status.to_string(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    fn write_to(self, stream: &mut TcpStream) {
        let mut response = format!(
            "HTTP/1.1 {}\r\nConnection: close\r\nContent-Length: {}\r\n",
            self.status,
            self.body.len()
        );
        for (name, value) in self.headers {
            response.push_str(&format!("{name}: {value}\r\n"));
        }
        response.push_str("\r\n");
        stream
            .write_all(response.as_bytes())
            .expect("write response headers");
        if !self.body.is_empty() {
            stream.write_all(&self.body).expect("write response body");
        }
        stream.flush().expect("flush response");
    }
}

pub struct MockHttpServer {
    base_url: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    join_handle: Option<thread::JoinHandle<()>>,
}

impl MockHttpServer {
    pub fn spawn(responses: Vec<MockResponse>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock http server");
        let address = listener.local_addr().expect("mock http server address");
        Self::spawn_with_listener(listener, format!("http://{address}"), responses)
    }

    pub fn spawn_with_builder<F>(build: F) -> Self
    where
        F: FnOnce(&str) -> Vec<MockResponse>,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock http server");
        let address = listener.local_addr().expect("mock http server address");
        let base_url = format!("http://{address}");
        let responses = build(&base_url);
        Self::spawn_with_listener(listener, base_url, responses)
    }

    fn spawn_with_listener(
        listener: TcpListener,
        base_url: String,
        responses: Vec<MockResponse>,
    ) -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_store = Arc::clone(&requests);
        let mut queued = VecDeque::from(responses);
        let join_handle = thread::spawn(move || {
            while let Some(response) = queued.pop_front() {
                let (mut stream, _) = listener.accept().expect("accept mock request");
                let request = read_request(&mut stream).expect("read mock request");
                request_store
                    .lock()
                    .expect("lock request store")
                    .push(request);
                response.write_to(&mut stream);
            }
        });

        Self {
            base_url,
            requests,
            join_handle: Some(join_handle),
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.requests
            .lock()
            .expect("lock recorded requests")
            .clone()
    }
}

impl Drop for MockHttpServer {
    fn drop(&mut self) {
        let _ = self.join_handle.take();
    }
}

pub struct TestDir {
    path: PathBuf,
}

impl TestDir {
    pub fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!("npc-labeler-{label}-{}", Uuid::new_v4()));
        create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = remove_dir_all(&self.path);
    }
}

fn read_request(stream: &mut TcpStream) -> std::io::Result<RecordedRequest> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    let header_end;
    loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(std::io::Error::other(
                "unexpected EOF while reading request",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(index) = find_subsequence(&buffer, b"\r\n\r\n") {
            header_end = index + 4;
            break;
        }
    }

    let header_text = String::from_utf8_lossy(&buffer[..header_end]);
    let mut lines = header_text.split("\r\n").filter(|line| !line.is_empty());
    let request_line = lines
        .next()
        .ok_or_else(|| std::io::Error::other("missing request line"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| std::io::Error::other("missing request method"))?
        .to_string();
    let path = parts
        .next()
        .ok_or_else(|| std::io::Error::other("missing request path"))?
        .to_string();

    let mut content_length = 0_usize;
    let mut chunked = false;
    let mut expect_continue = false;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse::<usize>().unwrap_or(0);
            }
            if name.eq_ignore_ascii_case("transfer-encoding")
                && value.trim().eq_ignore_ascii_case("chunked")
            {
                chunked = true;
            }
            if name.eq_ignore_ascii_case("expect")
                && value.trim().eq_ignore_ascii_case("100-continue")
            {
                expect_continue = true;
            }
        }
    }

    if expect_continue {
        stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
        stream.flush()?;
    }

    let body = if chunked {
        read_chunked_body(stream, buffer[header_end..].to_vec())?
    } else {
        let mut body = buffer[header_end..].to_vec();
        while body.len() < content_length {
            let read = stream.read(&mut chunk)?;
            if read == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..read]);
        }
        body.truncate(content_length);
        body
    };

    Ok(RecordedRequest { method, path, body })
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn read_chunked_body(stream: &mut TcpStream, mut buffer: Vec<u8>) -> std::io::Result<Vec<u8>> {
    let mut chunk = [0_u8; 1024];
    let mut offset = 0_usize;
    let mut body = Vec::new();

    loop {
        while find_subsequence(&buffer[offset..], b"\r\n").is_none() {
            let read = stream.read(&mut chunk)?;
            if read == 0 {
                return Err(std::io::Error::other("unexpected EOF in chunk header"));
            }
            buffer.extend_from_slice(&chunk[..read]);
        }

        let header_rel_end = find_subsequence(&buffer[offset..], b"\r\n").expect("chunk header");
        let header_end = offset + header_rel_end;
        let size_text = String::from_utf8_lossy(&buffer[offset..header_end]);
        let size = usize::from_str_radix(size_text.trim(), 16)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        offset = header_end + 2;

        if size == 0 {
            while buffer.len() < offset + 2 {
                let read = stream.read(&mut chunk)?;
                if read == 0 {
                    break;
                }
                buffer.extend_from_slice(&chunk[..read]);
            }
            break;
        }

        while buffer.len() < offset + size + 2 {
            let read = stream.read(&mut chunk)?;
            if read == 0 {
                return Err(std::io::Error::other("unexpected EOF in chunk body"));
            }
            buffer.extend_from_slice(&chunk[..read]);
        }

        body.extend_from_slice(&buffer[offset..offset + size]);
        offset += size + 2;
    }

    Ok(body)
}
