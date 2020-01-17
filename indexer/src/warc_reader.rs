use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read};
use std::str;

pub struct WARC {
    pub url: String,
    pub text: String,
}

pub struct WARCReader<R: Read> {
    header: BTreeMap<String, String>,
    buffer: Vec<u8>,
    reader: BufReader<R>,
}

impl<R: Read> Iterator for WARCReader<R> {
    type Item = WARC;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !self.read() {
                break;
            }
            if let Some(url) = self.url() {
                return Some(WARC {
                    url: url.clone(),
                    text: self.content().to_string(),
                });
            }
        }
        None
    }
}

impl<R: Read> WARCReader<R> {
    pub fn new(r: R) -> WARCReader<R> {
        WARCReader {
            header: BTreeMap::new(),
            buffer: Vec::new(),
            reader: BufReader::new(r),
        }
    }

    pub fn url(&self) -> Option<&String> {
        self.header.get("WARC-Target-URI")
    }

    pub fn content(&self) -> &str {
        str::from_utf8(&self.buffer).expect("Content is not utf8")
    }

    pub fn read(&mut self) -> bool {
        self.header.clear();
        let mut line = String::new();
        while self.reader.read_line(&mut line).expect("io error") > 0 {
            if !line.trim().is_empty() {
                break;
            }
            line.clear();
        }
        if line.trim() != "WARC/1.0" {
            return false;
        }
        line.clear();
        while self.reader.read_line(&mut line).expect("io error") > 0 {
            {
                let fields = line.trim().splitn(2, ":").collect::<Vec<&str>>();
                if fields.len() == 2 {
                    self.header
                        .insert(fields[0].to_string(), fields[1].trim().to_string());
                } else {
                    break;
                }
            }
            line.clear();
        }
        let content_len_str = self
            .header
            .get("Content-Length")
            .expect("Content length not found");
        let content_len: usize = content_len_str
            .parse()
            .expect("Failed to parse content len");
        self.buffer.resize(content_len, 0u8);
        self.reader
            .read_exact(&mut self.buffer[..])
            .expect("Failed to read content");
        true
    }
}
