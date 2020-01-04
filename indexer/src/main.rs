extern crate curl;
extern crate structopt;
extern crate structopt_derive;

#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate tantivy;
extern crate chan;
extern crate flate2;
extern crate futures;
extern crate indicatif;
extern crate itertools;
extern crate rayon;
extern crate serde_json;
extern crate whatlang;

use flate2::read::MultiGzDecoder;
use futures::future::Future;
use indicatif::ProgressBar;
mod warc_reader;
use self::warc_reader::WARCReader;
use chan::Receiver;
use curl::easy;
use indicatif::MultiProgress;
use indicatif::ProgressStyle;
use itertools::Itertools;
use std::fs;
use std::mem;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use structopt::StructOpt;
use tantivy::schema::{
    IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, STORED,
};
use tantivy::tokenizer::{
    AlphaNumOnlyFilter, Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, Tokenizer,
};
use tantivy::{Index, IndexWriter};

/// Number of WET files in a commit.
const CHUNK_SIZE: usize = 10;

const URL_ROOT: &'static str = "https://commoncrawl.s3.amazonaws.com/crawl-data/";

const WAIT_AFTER_RETRY_SECONDS: u64 = 30u64;

#[derive(StructOpt, Clone, Debug)]
struct CliOption {
    /// Needed parameter, the first on the command line.
    #[structopt(
        short = "i",
        long = "index",
        help = "Index directory",
        parse(from_os_str)
    )]
    pub index_directory: PathBuf,

    #[structopt(
        short = "s",
        long = "shard",
        default_value = "0",
        help = "Shard id (number between 1-80)"
    )]
    pub shard_id: usize,

    #[structopt(
        short = "ns",
        long = "nshards",
        default_value = "360",
        help = "Total num shards"
    )]
    pub total_num_shards: usize,

    #[structopt(
        short = "t",
        long = "num_threads",
        default_value = "2",
        help = "Number of threads"
    )]
    pub num_threads: usize,

    #[structopt(
        short = "m",
        long = "mem",
        default_value = "2000",
        help = "Amount of memory for the indexer heap"
    )]
    pub memory_in_mb: usize,
}

#[derive(Debug)]
struct WetFiles {
    files: Vec<String>,
}

impl WetFiles {
    fn for_shard_id(shard_id: usize, num_per_shards: usize) -> WetFiles {
        let urls_text = include_str!("urls-list.txt");
        let urls: Vec<String> = urls_text.lines().map(|s| s.to_string()).collect();
        let start_idx = num_per_shards * shard_id;
        let stop_idx = (start_idx + num_per_shards).min(urls.len());
        println!("Range {} {}", start_idx, stop_idx);
        let urls = urls[start_idx..stop_idx].to_owned();
        assert!(!urls.is_empty());
        WetFiles { files: urls }
    }

    fn len(&self) -> usize {
        self.files.len()
    }

    fn skip_to(&mut self, checkpoint: &str) {
        let pos = self
            .files
            .iter()
            .position(|s| s == checkpoint)
            .expect(&format!("Failed to find checkpoint {:?}", checkpoint));
        self.files = self.files[pos + 1..].to_owned();
    }

    fn files(&self) -> &[String] {
        &self.files[..]
    }
}

fn schema() -> Schema {
    let mut schema_builder = SchemaBuilder::new();
    let text_indexing_options = TextFieldIndexing::default()
        .set_index_option(IndexRecordOption::WithFreqsAndPositions)
        .set_tokenizer("en_stem");
    let text_options = TextOptions::default()
        .set_indexing_options(text_indexing_options)
        .set_stored();
    schema_builder.add_text_field("text", text_options);
    schema_builder.add_text_field("url", STORED);
    schema_builder.build()
}

fn init(index_directory: &Path, shard_id: usize) -> tantivy::Result<PathBuf> {
    let shard_subdir = format!("shard_{:03}", shard_id);
    let shard_directory = index_directory.join(&shard_subdir);
    if !shard_directory.exists() {
        fs::create_dir(&shard_directory)?;
        Index::create_in_dir(&shard_directory, schema())?;
    }
    Ok(shard_directory)
}

pub struct WetData {
    pub wet_file: String,
    pub data: Vec<u8>,
}

impl WetData {
    fn data(&self) -> &[u8] {
        &self.data[..]
    }
}

struct DownloadToBuffer {
    data: Vec<u8>,
    progress_bar: ProgressBar,
}

impl easy::Handler for DownloadToBuffer {
    fn write(&mut self, data: &[u8]) -> Result<usize, easy::WriteError> {
        self.data.extend_from_slice(data);
        // self.progress_bar.inc(data.len() as u64);
        Ok(data.len())
    }

    fn progress(&mut self, dltotal: f64, dlnow: f64, _ultotal: f64, _ulnow: f64) -> bool {
        self.progress_bar.set_length(dltotal as u64);
        self.progress_bar.set_position(dlnow as u64);
        true
    }
}

impl DownloadToBuffer {
    fn purge(&mut self) -> Vec<u8> {
        mem::replace(&mut self.data, Vec::new())
    }
}

fn download(url: &str, progress_bar: ProgressBar) -> Result<Vec<u8>, curl::Error> {
    progress_bar.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.yellow/blue}] {bytes}/{total_bytes} ({eta})")
        .progress_chars("#>-"));
    let collector = DownloadToBuffer {
        data: Vec::new(),
        progress_bar: progress_bar,
    };
    let mut easy = easy::Easy2::new(collector);
    easy.get(true)?;
    easy.url(&url)?;
    easy.progress(true)?;
    easy.perform()?;
    assert_eq!(easy.response_code()?, 200);
    Ok(easy.get_mut().purge())
}

fn download_with_retry(
    url: &str,
    num_retries: usize,
    progress_bars: Arc<MultiProgress>,
) -> Result<Vec<u8>, curl::Error> {
    assert!(num_retries > 0);
    for _ in 0..(num_retries - 1) {
        let progress_bar = progress_bars.add(ProgressBar::new(120_000_000));
        if let Ok(buffer) = download(url, progress_bar) {
            return Ok(buffer);
        } else {
            warn!("Failed, retrying!");
            thread::sleep(Duration::from_secs(WAIT_AFTER_RETRY_SECONDS))
        }
    }
    let progress_bar = progress_bars.add(ProgressBar::new(120_000_000));
    download(url, progress_bar)
}

fn download_wet(
    wet_files: WetFiles,
    send: chan::Sender<WetData>,
    progress_bars: Arc<MultiProgress>,
) -> Result<(), curl::Error> {
    for wet_file in wet_files.files() {
        let url = format!("{}{}", URL_ROOT, wet_file);
        let wet_data: Vec<u8> = download_with_retry(&url, 10, progress_bars.clone())?;
        send.send(WetData {
            wet_file: wet_file.clone(),
            data: wet_data,
        })
    }
    Ok(())
}

fn is_english(text: &str) -> bool {
    const SAMPLE_SIZE: usize = 500;
    let (start, stop) = {
        // detecting language is actually quite expensive. We only take 1000 byte in the middle.
        // because it is utf8 we need some logic to avoid cutting in the middle of a codepoint.
        if text.len() > SAMPLE_SIZE + 8 {
            let target_start = (text.len() - SAMPLE_SIZE) / 2;
            let utf8_start = |target: usize| {
                (0..4)
                    .map(|i| target - i)
                    .filter(|i| text.as_bytes()[*i] & 0b1100_0000 != 0b1000_0000)
                    .next()
            };
            let start = utf8_start(target_start).unwrap_or(0);
            let stop = utf8_start(target_start + SAMPLE_SIZE).unwrap_or(text.len());
            (start, stop)
        } else {
            (0, text.len())
        }
    };
    let sample = &text[start..stop];
    if let Some(whatlang::Script::Latin) = whatlang::detect_script(sample) {
        let options = whatlang::Options::new().set_whitelist(vec![
            whatlang::Lang::Eng,
            whatlang::Lang::Spa,
            whatlang::Lang::Deu,
        ]);
        if let Some(info) = whatlang::detect_with_options(sample, &options) {
            return info.is_reliable();
        }
    }
    return false;
}

fn index_wet_file(wet_data: &WetData, schema: &Schema, index_writer: &mut IndexWriter) {
    let mut cursor: &[u8] = wet_data.data();
    let decoder = MultiGzDecoder::new(&mut cursor);
    let warc_reader = WARCReader::new(decoder);
    let url_field = schema.get_field("url").expect("url field not in schema");
    let text_field = schema.get_field("text").expect("text field not in schema");
    for warc in warc_reader {
        if !is_english(&warc.text) {
            // we only index english content.
            continue;
        }
        index_writer.add_document(doc!(
            url_field => warc.url,
            text_field => warc.text
        ));
    }
}

fn main() -> tantivy::Result<()> {
    env_logger::init().unwrap();
    let cli_options = CliOption::from_args();
    resume_indexing(&cli_options)?;
    println!("Indexing succeeded indexing");
    merge_all(&cli_options)?;
    Ok(())
}

fn merge_all(cli_options: &CliOption) -> tantivy::Result<()> {
    let index_directory = init(&cli_options.index_directory, cli_options.shard_id)?;
    let index = Index::open_in_dir(index_directory)?;
    let segment_ids = index.searchable_segment_ids()?;
    let mut index_writer = index.writer_with_num_threads(1, 10_000_000)?;
    index_writer.garbage_collect_files()?;
    if segment_ids.len() > 1 {
        index_writer
            .merge(&segment_ids)?
            .wait()
            .expect("Merge failed");
    }
    index_writer.garbage_collect_files()?;
    Ok(())
}

fn indexing_wet_queue(
    index: Index,
    wet_queue: Receiver<WetData>,
    progress_bar: ProgressBar,
    cli_options: &CliOption,
) -> tantivy::Result<()> {
    progress_bar.tick();
    let schema = index.schema();
    let mut index_writer = index.writer_with_num_threads(
        cli_options.num_threads,
        cli_options.memory_in_mb * 1_000_000,
    )?;

    for wet_files in wet_queue.into_iter().chunks(CHUNK_SIZE).into_iter() {
        let mut checkpoint = String::new();
        for wet_data in wet_files {
            index_wet_file(&wet_data, &schema, &mut index_writer);
            checkpoint = wet_data.wet_file.clone();
            progress_bar.inc(1u64);
        }

        let mut prepared_commit = index_writer.prepare_commit()?;
        prepared_commit.set_payload(&checkpoint);
        prepared_commit.commit()?;
    }
    progress_bar.finish();

    index_writer.wait_merging_threads()?;

    Ok(())
}

fn resume_indexing(cli_options: &CliOption) -> tantivy::Result<()> {
    let cli_options: CliOption = (*cli_options).clone();
    let num_per_shards = (80_000 + cli_options.total_num_shards - 1) / cli_options.total_num_shards;
    let mut wet_files = WetFiles::for_shard_id(cli_options.shard_id, num_per_shards);
    let index_directory = init(&cli_options.index_directory, cli_options.shard_id)?;
    let index = Index::open_in_dir(index_directory)?;
    // overriding `en_stem` to remove alphanum only characters.
    // ... That way it will only affect indexing and not querying.
    index.tokenizers().register(
        "en_stem",
        SimpleTokenizer
            .filter(RemoveLongFilter::limit(40))
            .filter(LowerCaser)
            .filter(AlphaNumOnlyFilter)
            .filter(Stemmer::new(Language::English)),
    );

    let index_metas = index.load_metas()?;
    if let Some(checkpoint) = index_metas.payload {
        info!("Resuming at {:?}", checkpoint);
        wet_files.skip_to(&checkpoint);
    }

    let num_wet_files_remaining = wet_files.len();
    let num_wet_files_indexed = num_per_shards - num_wet_files_remaining;

    let progress_bars = Arc::new(MultiProgress::new());

    let (send, recv) = chan::sync(1);
    let progress_bars_clone = progress_bars.clone();
    let _download_thread = thread::spawn(move || {
        download_wet(wet_files, send, progress_bars_clone).unwrap();
    });

    let index_progress_bar = progress_bars.add(ProgressBar::new(num_per_shards as u64));
    index_progress_bar.set_style(ProgressStyle::default_bar().template(
        "{spinner:.green} [{elapsed_precise}] [{bar:40.yellow/blue}] {pos:>7}/{len:7} {eta}",
    ));

    index_progress_bar.set_position(num_wet_files_indexed as u64);

    {
        index.writer_with_num_threads(1, 30_000_000)?;
    }
    let _indexing_thread = thread::spawn(move || {
        indexing_wet_queue(index, recv, index_progress_bar, &cli_options).unwrap();
    });

    progress_bars.join_and_clear().unwrap();

    Ok(())
}
