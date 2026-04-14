use std::collections::HashMap;
use std::f64::consts::{FRAC_PI_2, TAU};
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use flate2::read::GzDecoder;
use plotters::coord::Shift;
use plotters::prelude::*;
use plotters::style::{FontStyle, register_font};
use serde::Deserialize;
use zenodo_rs::{ArtifactSelector, Auth, ZenodoClient};
use zstd::stream::read::Decoder as ZstdDecoder;

const DEFAULT_ZENODO_DOI: &str = "10.5281/zenodo.14040990";
const WIDTH: u32 = 1_600;
const HEADER_HEIGHT: u32 = 108;
const METRICS_ROW_HEIGHT: u32 = 292;
const CIRCLE_CENTER_Y_OFFSET: i32 = 24;

const BG: RGBColor = RGBColor(255, 255, 255);
const PANEL_BG: RGBColor = RGBColor(255, 255, 255);
const PANEL_BORDER: RGBColor = RGBColor(220, 214, 203);
const TEXT: RGBColor = RGBColor(41, 53, 65);
const MUTED: RGBColor = RGBColor(107, 117, 128);
const COVERAGE_COLOR: RGBColor = RGBColor(35, 137, 142);
const REMAINING_COLOR: RGBColor = RGBColor(206, 213, 221);
const SUCCESS_COLOR: RGBColor = RGBColor(54, 136, 88);
const EMPTY_COLOR: RGBColor = RGBColor(130, 184, 142);
const INVALID_COLOR: RGBColor = RGBColor(223, 170, 72);
const FAILED_COLOR: RGBColor = RGBColor(200, 93, 73);
const UNSUCCESSFUL_COLOR: RGBColor = RGBColor(200, 93, 73);
const PATHWAY_COLOR: RGBColor = RGBColor(59, 143, 168);
const SUPERCLASS_COLOR: RGBColor = RGBColor(218, 149, 58);
const CLASS_COLOR: RGBColor = RGBColor(178, 88, 71);
const FONT_ENV: &str = "NPC_LABELER_FONT_PATH";
const FONT_CANDIDATES: &[&str] = &[
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/TTF/DejaVuSans.ttf",
];

#[derive(Parser, Debug)]
#[command(name = "npc-progress")]
#[command(about = "Render a single progress dashboard from the published Zenodo snapshot")]
struct Args {
    /// Where to write the rendered dashboard (.svg or .png).
    #[arg(long, default_value = "progress.svg")]
    output: PathBuf,

    /// Path to the `PubChem` CID-SMILES input used to define the total crawl universe.
    #[arg(long, default_value = "CID-SMILES.gz")]
    input: PathBuf,

    /// Optional explicit `PubChem` total. Use this to avoid rescanning the input file.
    #[arg(long)]
    pubchem_total: Option<u64>,

    /// Zenodo DOI or concept DOI for the published snapshot family.
    #[arg(long, default_value = DEFAULT_ZENODO_DOI)]
    zenodo_doi: String,

    /// Cache directory used when downloading snapshot artifacts from Zenodo.
    #[arg(long, default_value = "cache/progress")]
    cache_dir: PathBuf,

    /// How many top labels to list per taxonomy layer panel.
    #[arg(long, default_value_t = 8)]
    top_n: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct StatusCounts {
    classified: u64,
    empty: u64,
    invalid: u64,
    failed: u64,
    pending: u64,
}

impl StatusCounts {
    #[inline]
    fn successful(self) -> u64 {
        self.classified + self.empty
    }

    #[inline]
    fn unsuccessful(self) -> u64 {
        self.invalid + self.failed
    }

    #[inline]
    fn executed(self) -> u64 {
        self.successful() + self.unsuccessful()
    }

    #[inline]
    fn total(self) -> u64 {
        self.executed() + self.pending
    }
}

#[derive(Debug, Clone, Copy)]
struct RequestMetrics {
    total_requests: u64,
    successful_requests: u64,
    invalid_responses: u64,
    unsuccessful_requests: u64,
}

#[derive(Debug, Clone)]
struct MetricSlice {
    label: String,
    value: u64,
    color: RGBColor,
}

#[derive(Debug, Clone)]
struct MetricPanel {
    title: String,
    subtitle: String,
    center_value: String,
    center_caption: String,
    footer_note: Option<String>,
    slices: Vec<MetricSlice>,
}

#[derive(Debug, Clone)]
struct OverlayTextLine {
    text: String,
    center_x: i32,
    y: i32,
    font_size: f64,
    color: RGBColor,
}

#[derive(Debug, Clone)]
struct MetricOverlay {
    center: (i32, i32),
    outer_radius: f64,
    inner_radius: f64,
    slices: Vec<MetricSlice>,
    text_lines: Vec<OverlayTextLine>,
}

#[derive(Debug, Clone)]
struct RingOverlay {
    outer_radius: f64,
    inner_radius: f64,
    value: u64,
    total: u64,
    color: RGBColor,
    background: RGBColor,
}

#[derive(Debug, Clone)]
struct DepthOverlay {
    center: (i32, i32),
    rings: Vec<RingOverlay>,
    text_lines: Vec<OverlayTextLine>,
}

#[derive(Debug, Clone)]
enum SvgOverlay {
    Metric(MetricOverlay),
    Depth(DepthOverlay),
}

#[derive(Debug, Clone)]
struct Snapshot {
    source_label: String,
    timestamp_label: Option<String>,
    counts: StatusCounts,
    request_metrics: Option<RequestMetrics>,
    layer_coverage: LayerCoverage,
    layers: [LayerBreakdown; 3],
}

impl Snapshot {
    fn validate(&self) -> io::Result<()> {
        if self.counts.executed() > self.counts.total() {
            return Err(io::Error::other(
                "executed row count exceeds the reported PubChem total",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct LayerBreakdown {
    title: &'static str,
    assignment_total: u64,
    labels: Vec<CountBucket>,
}

impl LayerBreakdown {
    #[inline]
    fn distinct_labels(&self) -> usize {
        self.labels.len()
    }

    fn top_buckets(&self, limit: usize) -> &[CountBucket] {
        &self.labels[..self.labels.len().min(limit)]
    }

    fn tail_count(&self, limit: usize) -> u64 {
        self.labels
            .iter()
            .skip(limit)
            .map(|bucket| bucket.count)
            .sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CountBucket {
    label: String,
    count: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct LayerCoverage {
    pathway: u64,
    superclass: u64,
    class: u64,
}

#[derive(Debug, Default)]
struct LayerAccumulator {
    classified_rows: u64,
    empty_rows: u64,
    pathway_rows: u64,
    superclass_rows: u64,
    class_rows: u64,
    pathway_counts: HashMap<String, u64>,
    superclass_counts: HashMap<String, u64>,
    class_counts: HashMap<String, u64>,
}

impl LayerAccumulator {
    fn record(&mut self, record: CompletedRecord) {
        let has_pathway = !record.pathways.is_empty();
        let has_superclass = !record.superclasses.is_empty();
        let has_class = !record.classes.is_empty();
        let has_labels = has_pathway || has_superclass || has_class;
        if has_labels {
            self.classified_rows += 1;
        } else {
            self.empty_rows += 1;
        }

        if has_pathway {
            self.pathway_rows += 1;
        }
        if has_superclass {
            self.superclass_rows += 1;
        }
        if has_class {
            self.class_rows += 1;
        }

        tally_labels(&mut self.pathway_counts, record.pathways);
        tally_labels(&mut self.superclass_counts, record.superclasses);
        tally_labels(&mut self.class_counts, record.classes);
    }

    fn into_layers(self) -> (u64, u64, LayerCoverage, [LayerBreakdown; 3]) {
        (
            self.classified_rows,
            self.empty_rows,
            LayerCoverage {
                pathway: self.pathway_rows,
                superclass: self.superclass_rows,
                class: self.class_rows,
            },
            [
                finalize_breakdown("Pathway", self.pathway_counts),
                finalize_breakdown("Superclass", self.superclass_counts),
                finalize_breakdown("Class", self.class_counts),
            ],
        )
    }
}

#[derive(Debug, Deserialize)]
struct CompletedRecord {
    #[serde(rename = "class_results")]
    classes: Vec<String>,
    #[serde(rename = "superclass_results")]
    superclasses: Vec<String>,
    #[serde(rename = "pathway_results")]
    pathways: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    created_at: String,
    output_filename: String,
    successful_rows: u64,
    invalid_rows: u64,
    failed_rows: u64,
    #[serde(default)]
    pubchem_total: Option<u64>,
    #[serde(default)]
    total_requests: Option<u64>,
    #[serde(default)]
    successful_requests: Option<u64>,
    #[serde(default)]
    invalid_requests: Option<u64>,
    #[serde(default)]
    failed_requests: Option<u64>,
}

fn main() {
    let args = Args::parse();
    if let Err(error) = run(&args) {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

fn run(args: &Args) -> io::Result<()> {
    if args.top_n == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--top-n must be at least 1",
        ));
    }

    let snapshot = load_from_zenodo(args)?;
    snapshot.validate()?;
    render_snapshot(&snapshot, &args.output, args.top_n)
}

fn load_from_zenodo(args: &Args) -> io::Result<Snapshot> {
    fs::create_dir_all(&args.cache_dir)?;
    let slug = sanitize_identifier(&args.zenodo_doi);
    let cache_dir = args.cache_dir.join(slug);
    fs::create_dir_all(&cache_dir)?;

    let manifest_path = cache_dir.join("manifest.json");
    download_latest_zenodo_file(&args.zenodo_doi, "manifest.json", &manifest_path)?;
    let manifest: Manifest =
        serde_json::from_reader(BufReader::new(File::open(&manifest_path)?)).map_err(json_error)?;

    let dataset_path = cache_dir.join(&manifest.output_filename);
    download_latest_zenodo_file(&args.zenodo_doi, &manifest.output_filename, &dataset_path)?;

    let total_pubchem = resolve_pubchem_total(args, manifest.pubchem_total)?;
    let accumulator = accumulate_completed_dataset(&dataset_path)?;
    let (classified, empty, layer_coverage, layers) = accumulator.into_layers();
    let successful = classified + empty;
    if successful != manifest.successful_rows {
        return Err(io::Error::other(format!(
            "manifest says {} successful rows but parsed {successful}",
            manifest.successful_rows
        )));
    }

    let executed = successful + manifest.invalid_rows + manifest.failed_rows;
    if executed > total_pubchem {
        return Err(io::Error::other(format!(
            "processed {executed} rows but total PubChem rows is {total_pubchem}"
        )));
    }

    Ok(Snapshot {
        source_label: format!("latest Zenodo snapshot ({})", args.zenodo_doi),
        timestamp_label: Some(manifest.created_at.clone()),
        request_metrics: manifest_request_metrics(&manifest),
        layer_coverage,
        counts: StatusCounts {
            classified,
            empty,
            invalid: manifest.invalid_rows,
            failed: manifest.failed_rows,
            pending: total_pubchem - executed,
        },
        layers,
    })
}

fn resolve_pubchem_total(args: &Args, hinted_total: Option<u64>) -> io::Result<u64> {
    if let Some(total) = args.pubchem_total.or(hinted_total) {
        return Ok(total);
    }

    if args.input.exists() {
        return count_input_rows(&args.input);
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "unable to determine total PubChem rows; pass --pubchem-total or provide CID-SMILES.gz",
    ))
}

fn accumulate_completed_dataset(path: &Path) -> io::Result<LayerAccumulator> {
    let decoder = ZstdDecoder::new(File::open(path)?)
        .map_err(|error| io::Error::other(format!("cannot open {}: {error}", path.display())))?;
    let reader = BufReader::new(decoder);
    let mut accumulator = LayerAccumulator::default();
    accumulate_reader(reader, &mut accumulator)?;
    Ok(accumulator)
}

fn accumulate_reader<R: BufRead>(reader: R, accumulator: &mut LayerAccumulator) -> io::Result<()> {
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record: CompletedRecord = serde_json::from_str(&line).map_err(json_error)?;
        accumulator.record(record);
    }
    Ok(())
}

fn count_input_rows(path: &Path) -> io::Result<u64> {
    let mut total = 0_u64;
    let mut line = String::new();
    let mut reader = open_input_reader(path)?;
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        if parse_input_line(&line).is_some() {
            total += 1;
        }
    }
    Ok(total)
}

fn open_input_reader(path: &Path) -> io::Result<Box<dyn BufRead>> {
    let file = File::open(path)?;
    if path
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("gz"))
    {
        return Ok(Box::new(BufReader::with_capacity(
            8 * 1024 * 1024,
            GzDecoder::new(file),
        )));
    }
    Ok(Box::new(BufReader::with_capacity(8 * 1024 * 1024, file)))
}

fn ensure_plotters_font() -> io::Result<()> {
    static FONT_INIT: OnceLock<Result<(), String>> = OnceLock::new();

    FONT_INIT
        .get_or_init(|| initialize_plotters_font().map_err(|error| error.to_string()))
        .clone()
        .map_err(io::Error::other)
}

fn initialize_plotters_font() -> io::Result<()> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some(path) = std::env::var_os(FONT_ENV) {
        candidates.push(PathBuf::from(path));
    }
    candidates.extend(FONT_CANDIDATES.iter().map(PathBuf::from));

    for candidate in &candidates {
        if !candidate.is_file() {
            continue;
        }

        let bytes = fs::read(candidate)?;
        let leaked = Box::leak(bytes.into_boxed_slice());
        register_font("sans-serif", FontStyle::Normal, leaked).map_err(|_| {
            io::Error::other(format!(
                "failed to register plotters font from {}",
                candidate.display()
            ))
        })?;
        return Ok(());
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "no usable TTF font found for PNG text rendering; set {FONT_ENV} or install DejaVu Sans"
        ),
    ))
}

fn parse_input_line(line: &str) -> Option<(u64, &str)> {
    let (cid, smiles) = line.split_once('\t')?;
    let cid = cid.parse::<u64>().ok()?;
    Some((cid, smiles.trim()))
}

fn download_latest_zenodo_file(doi: &str, key: &str, destination: &Path) -> io::Result<()> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }

    let token = std::env::var("ZENODO_TOKEN").unwrap_or_default();
    let client = ZenodoClient::builder(Auth::new(token))
        .user_agent(format!(
            "{}/{}",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION")
        ))
        .request_timeout(Duration::from_mins(2))
        .connect_timeout(Duration::from_secs(20))
        .build()
        .map_err(|error| io::Error::other(format!("failed to build Zenodo client: {error}")))?;
    let selector = ArtifactSelector::latest_file_by_doi(doi, key).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid Zenodo DOI {doi}: {error}"),
        )
    })?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| io::Error::other(format!("failed to build tokio runtime: {error}")))?;
    runtime
        .block_on(async move { client.download_artifact(&selector, destination).await })
        .map_err(|error| {
            io::Error::other(format!("failed to download {key} from Zenodo: {error}"))
        })?;

    Ok(())
}

fn tally_labels(target: &mut HashMap<String, u64>, labels: Vec<String>) {
    for label in labels {
        *target.entry(label).or_insert(0) += 1;
    }
}

fn manifest_request_metrics(manifest: &Manifest) -> Option<RequestMetrics> {
    let total_requests = manifest.total_requests?;
    let successful_requests = manifest.successful_requests?;
    let invalid_responses = manifest.invalid_requests.unwrap_or(0);
    let unsuccessful_requests = manifest.failed_requests?;
    Some(RequestMetrics {
        total_requests,
        successful_requests,
        invalid_responses,
        unsuccessful_requests,
    })
}

fn finalize_breakdown(title: &'static str, counts: HashMap<String, u64>) -> LayerBreakdown {
    let assignment_total: u64 = counts.values().copied().sum();
    let mut labels: Vec<_> = counts
        .into_iter()
        .map(|(label, count)| CountBucket { label, count })
        .collect();
    labels.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.label.cmp(&right.label))
    });

    LayerBreakdown {
        title,
        assignment_total,
        labels,
    }
}

fn render_snapshot(snapshot: &Snapshot, output: &Path, top_n: usize) -> io::Result<()> {
    if let Some(parent) = output.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }

    ensure_plotters_font()?;
    let height = dashboard_height(snapshot, top_n);

    match output
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("svg") | None => {
            let root = SVGBackend::new(output, (WIDTH, height)).into_drawing_area();
            let mut overlays = Vec::with_capacity(3);
            render_area(&root, snapshot, top_n, true, &mut overlays)?;
            inject_svg_overlays(output, &overlays)
        }
        Some("png") => {
            let root = BitMapBackend::new(output, (WIDTH, height)).into_drawing_area();
            let mut overlays = Vec::new();
            render_area(&root, snapshot, top_n, false, &mut overlays)
        }
        Some(other) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported output format .{other}; use .svg or .png"),
        )),
    }
}

fn render_area<DB: DrawingBackend>(
    root: &DrawingArea<DB, Shift>,
    snapshot: &Snapshot,
    top_n: usize,
    collect_svg_overlays: bool,
    overlays: &mut Vec<SvgOverlay>,
) -> io::Result<()>
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    root.fill(&BG).map_err(plotters_error)?;

    let (header, body) = root.split_vertically(HEADER_HEIGHT);
    let (metrics_row, layers_row) = body.split_vertically(METRICS_ROW_HEIGHT);
    let metric_panels = metrics_row.split_evenly((1, 3));
    let layer_panels = layers_row.split_evenly((1, 3));

    draw_header(&header, snapshot)?;
    draw_metric_panel(
        &metric_panels[0],
        &build_coverage_panel(snapshot),
        collect_svg_overlays,
        overlays,
    )?;
    draw_metric_panel(
        &metric_panels[1],
        &build_secondary_panel(snapshot),
        collect_svg_overlays,
        overlays,
    )?;
    draw_depth_panel(&metric_panels[2], snapshot, collect_svg_overlays, overlays)?;

    draw_layer_panel(&layer_panels[0], &snapshot.layers[0], PATHWAY_COLOR, top_n)?;
    draw_layer_panel(
        &layer_panels[1],
        &snapshot.layers[1],
        SUPERCLASS_COLOR,
        top_n,
    )?;
    draw_layer_panel(&layer_panels[2], &snapshot.layers[2], CLASS_COLOR, top_n)?;

    root.present().map_err(plotters_error)
}

fn build_coverage_panel(snapshot: &Snapshot) -> MetricPanel {
    MetricPanel {
        title: "Collected vs PubChem".to_string(),
        subtitle: format!(
            "{} successful rows from {} total PubChem compounds",
            format_number(snapshot.counts.successful()),
            format_number(snapshot.counts.total())
        ),
        center_value: format_percent(snapshot.counts.successful(), snapshot.counts.total()),
        center_caption: "collected".to_string(),
        footer_note: None,
        slices: vec![
            MetricSlice {
                label: format!("Collected: {}", format_number(snapshot.counts.successful())),
                value: snapshot.counts.successful(),
                color: COVERAGE_COLOR,
            },
            MetricSlice {
                label: format!("Pending: {}", format_number(snapshot.counts.pending)),
                value: snapshot.counts.pending,
                color: REMAINING_COLOR,
            },
            MetricSlice {
                label: format!("Invalid: {}", format_number(snapshot.counts.invalid)),
                value: snapshot.counts.invalid,
                color: INVALID_COLOR,
            },
            MetricSlice {
                label: format!("Failed: {}", format_number(snapshot.counts.failed)),
                value: snapshot.counts.failed,
                color: FAILED_COLOR,
            },
        ],
    }
}

fn build_secondary_panel(snapshot: &Snapshot) -> MetricPanel {
    if let Some(request_metrics) = snapshot.request_metrics {
        return MetricPanel {
            title: "Executed Request Outcomes".to_string(),
            subtitle: format!(
                "{} total request attempts captured by this source",
                format_number(request_metrics.total_requests)
            ),
            center_value: format_percent(
                request_metrics.successful_requests,
                request_metrics.total_requests,
            ),
            center_caption: "successful attempts".to_string(),
            footer_note: None,
            slices: vec![
                MetricSlice {
                    label: format!(
                        "Successful responses: {}",
                        format_number(request_metrics.successful_requests)
                    ),
                    value: request_metrics.successful_requests,
                    color: SUCCESS_COLOR,
                },
                MetricSlice {
                    label: format!(
                        "Invalid responses: {}",
                        format_number(request_metrics.invalid_responses)
                    ),
                    value: request_metrics.invalid_responses,
                    color: INVALID_COLOR,
                },
                MetricSlice {
                    label: format!(
                        "Other unsuccessful requests: {}",
                        format_number(request_metrics.unsuccessful_requests)
                    ),
                    value: request_metrics.unsuccessful_requests,
                    color: UNSUCCESSFUL_COLOR,
                },
            ],
        };
    }

    MetricPanel {
        title: "Terminal Row Outcomes".to_string(),
        subtitle: format!(
            "{} terminal rows recorded in this source",
            format_number(snapshot.counts.executed())
        ),
        center_value: format_percent(snapshot.counts.empty, snapshot.counts.executed()),
        center_caption: "empty rows".to_string(),
        footer_note: Some("Published manifest has no per-request totals.".to_string()),
        slices: vec![
            MetricSlice {
                label: format!(
                    "Classified rows: {}",
                    format_number(snapshot.counts.classified)
                ),
                value: snapshot.counts.classified,
                color: SUCCESS_COLOR,
            },
            MetricSlice {
                label: format!("Empty rows: {}", format_number(snapshot.counts.empty)),
                value: snapshot.counts.empty,
                color: EMPTY_COLOR,
            },
            MetricSlice {
                label: format!("Invalid rows: {}", format_number(snapshot.counts.invalid)),
                value: snapshot.counts.invalid,
                color: INVALID_COLOR,
            },
            MetricSlice {
                label: format!("Failed rows: {}", format_number(snapshot.counts.failed)),
                value: snapshot.counts.failed,
                color: FAILED_COLOR,
            },
        ],
    }
}

fn draw_header<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    snapshot: &Snapshot,
) -> io::Result<()>
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    let panel = prepare_panel(area)?;
    let title_style = TextStyle::from(("sans-serif", 31).into_font()).color(&TEXT);
    let description_style = TextStyle::from(("sans-serif", 17).into_font()).color(&MUTED);
    let meta_style = TextStyle::from(("sans-serif", 15).into_font()).color(&MUTED);
    let width = panel.dim_in_pixel().0 as i32;

    panel
        .draw(&Text::new(
            "NPC Labeler Progress Dashboard",
            (24, 34),
            title_style,
        ))
        .map_err(plotters_error)?;
    panel
        .draw(&Text::new(
            "Coverage, terminal outcomes, taxonomy depth, and label distribution for the current crawl snapshot.",
            (24, 60),
            description_style,
        ))
        .map_err(plotters_error)?;

    let timestamp_text = if let Some(label) = &snapshot.timestamp_label {
        format!("Snapshot timestamp: {label}")
    } else {
        format!("Rendered: {}", Utc::now().to_rfc3339())
    };
    let (timestamp_width, _) = panel
        .estimate_text_size(&timestamp_text, &meta_style)
        .map_err(plotters_error)?;
    let timestamp_x = width - 24 - timestamp_width as i32;
    let source_text = truncate_to_width(
        &panel,
        &format!("Source: {}", snapshot.source_label),
        &meta_style,
        (timestamp_x - 48).max(240) as u32,
    )?;
    panel
        .draw(&Text::new(source_text, (24, 80), meta_style.clone()))
        .map_err(plotters_error)?;
    panel
        .draw(&Text::new(timestamp_text, (timestamp_x, 80), meta_style))
        .map_err(plotters_error)?;

    Ok(())
}

fn draw_metric_panel<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    metric: &MetricPanel,
    collect_svg_overlays: bool,
    overlays: &mut Vec<SvgOverlay>,
) -> io::Result<()>
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    let panel = prepare_panel(area)?;
    panel
        .draw(&Text::new(
            metric.title.clone(),
            (24, 28),
            TextStyle::from(("sans-serif", 24).into_font()).color(&TEXT),
        ))
        .map_err(plotters_error)?;
    panel
        .draw(&Text::new(
            metric.subtitle.clone(),
            (24, 54),
            TextStyle::from(("sans-serif", 17).into_font()).color(&MUTED),
        ))
        .map_err(plotters_error)?;

    let footer_y = 80_i32;
    if let Some(footer_note) = &metric.footer_note {
        panel
            .draw(&Text::new(
                footer_note.clone(),
                (24, footer_y),
                TextStyle::from(("sans-serif", 14).into_font()).color(&MUTED),
            ))
            .map_err(plotters_error)?;
    }

    let total: u64 = metric.slices.iter().map(|slice| slice.value).sum();
    if total == 0 {
        panel
            .draw(&Text::new(
                "No rows available yet",
                (24, 112),
                TextStyle::from(("sans-serif", 20).into_font()).color(&TEXT),
            ))
            .map_err(plotters_error)?;
        return Ok(());
    }

    let dims = panel.dim_in_pixel();
    let center = (
        dims.0 as i32 - 180,
        dims.1 as i32 / 2 + CIRCLE_CENTER_Y_OFFSET,
    );
    let radius = (f64::from(dims.1) * 0.34)
        .min(f64::from(dims.0) * 0.16)
        .max(78.0);
    let hole_radius = radius * 0.68;
    if collect_svg_overlays {
        overlays.push(build_metric_overlay(
            &panel,
            metric,
            center,
            radius,
            hole_radius,
        ));
    } else {
        let slices: Vec<(u64, RGBColor)> = metric
            .slices
            .iter()
            .map(|slice| (slice.value, slice.color))
            .collect();
        draw_donut_chart(&panel, radius, hole_radius, center, &slices)?;
        let value_style = TextStyle::from(("sans-serif", 28).into_font()).color(&TEXT);
        let caption_style = TextStyle::from(("sans-serif", 15).into_font()).color(&MUTED);
        let (value_w, _) = panel
            .estimate_text_size(&metric.center_value, &value_style)
            .map_err(plotters_error)?;
        let (caption_w, _) = panel
            .estimate_text_size(&metric.center_caption, &caption_style)
            .map_err(plotters_error)?;
        panel
            .draw(&Text::new(
                metric.center_value.clone(),
                (center.0 - value_w as i32 / 2, center.1 - 18),
                value_style,
            ))
            .map_err(plotters_error)?;
        panel
            .draw(&Text::new(
                metric.center_caption.clone(),
                (center.0 - caption_w as i32 / 2, center.1 + 12),
                caption_style,
            ))
            .map_err(plotters_error)?;
    }

    let mut current_y = if metric.footer_note.is_some() {
        106
    } else {
        92
    };
    for slice in &metric.slices {
        draw_legend_entry(&panel, (24, current_y), slice.color, slice.label.clone())?;
        current_y += 24;
    }

    Ok(())
}

fn draw_depth_legend<DB: DrawingBackend>(
    panel: &DrawingArea<DB, Shift>,
    snapshot: &Snapshot,
    total: u64,
) -> io::Result<()>
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    draw_legend_entry(
        panel,
        (24, 110),
        PATHWAY_COLOR,
        format!(
            "Pathway rows: {} | {}",
            format_percent(snapshot.layer_coverage.pathway, total),
            format_number(snapshot.layer_coverage.pathway)
        ),
    )?;
    draw_legend_entry(
        panel,
        (24, 136),
        SUPERCLASS_COLOR,
        format!(
            "Superclass rows: {} | {}",
            format_percent(snapshot.layer_coverage.superclass, total),
            format_number(snapshot.layer_coverage.superclass)
        ),
    )?;
    draw_legend_entry(
        panel,
        (24, 162),
        CLASS_COLOR,
        format!(
            "Class rows: {} | {}",
            format_percent(snapshot.layer_coverage.class, total),
            format_number(snapshot.layer_coverage.class)
        ),
    )?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn draw_depth_panel<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    snapshot: &Snapshot,
    collect_svg_overlays: bool,
    overlays: &mut Vec<SvgOverlay>,
) -> io::Result<()>
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    let panel = prepare_panel(area)?;
    panel
        .draw(&Text::new(
            "Taxonomy Depth Coverage",
            (24, 28),
            TextStyle::from(("sans-serif", 24).into_font()).color(&TEXT),
        ))
        .map_err(plotters_error)?;
    panel
        .draw(&Text::new(
            "Share of successful rows with pathway, superclass, and class labels.",
            (24, 54),
            TextStyle::from(("sans-serif", 17).into_font()).color(&MUTED),
        ))
        .map_err(plotters_error)?;

    let total = snapshot.counts.successful();
    if total == 0 {
        panel
            .draw(&Text::new(
                "No successful rows available yet",
                (24, 112),
                TextStyle::from(("sans-serif", 20).into_font()).color(&TEXT),
            ))
            .map_err(plotters_error)?;
        return Ok(());
    }

    let dims = panel.dim_in_pixel();
    let center = (
        dims.0 as i32 - 150,
        dims.1 as i32 / 2 + CIRCLE_CENTER_Y_OFFSET,
    );
    let outer_radius = (f64::from(dims.1) * 0.34)
        .min(f64::from(dims.0) * 0.22)
        .max(84.0);
    let ring_width = (outer_radius * 0.18).max(12.0);
    let ring_gap = (outer_radius * 0.07).max(5.0);
    if collect_svg_overlays {
        overlays.push(build_depth_overlay(
            &panel,
            center,
            outer_radius,
            ring_width,
            ring_gap,
            snapshot,
            total,
        ));
    } else {
        draw_depth_ring(
            &panel,
            center,
            outer_radius,
            ring_width,
            snapshot.layer_coverage.pathway,
            total,
            PATHWAY_COLOR,
        )?;
        draw_depth_ring(
            &panel,
            center,
            outer_radius - ring_width - ring_gap,
            ring_width,
            snapshot.layer_coverage.superclass,
            total,
            SUPERCLASS_COLOR,
        )?;
        draw_depth_ring(
            &panel,
            center,
            outer_radius - 2.0 * (ring_width + ring_gap),
            ring_width,
            snapshot.layer_coverage.class,
            total,
            CLASS_COLOR,
        )?;
    }
    draw_depth_legend(&panel, snapshot, total)?;

    Ok(())
}

fn build_metric_overlay<DB: DrawingBackend>(
    panel: &DrawingArea<DB, Shift>,
    metric: &MetricPanel,
    center: (i32, i32),
    outer_radius: f64,
    inner_radius: f64,
) -> SvgOverlay
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    let (x_range, y_range) = panel.get_pixel_range();
    let center_abs = (x_range.start + center.0, y_range.start + center.1);
    SvgOverlay::Metric(MetricOverlay {
        center: center_abs,
        outer_radius,
        inner_radius,
        slices: metric.slices.clone(),
        text_lines: vec![
            OverlayTextLine {
                text: metric.center_value.clone(),
                center_x: center_abs.0,
                y: center_abs.1 - 18,
                font_size: 28.0,
                color: TEXT,
            },
            OverlayTextLine {
                text: metric.center_caption.clone(),
                center_x: center_abs.0,
                y: center_abs.1 + 12,
                font_size: 15.0,
                color: MUTED,
            },
        ],
    })
}

fn build_depth_overlay<DB: DrawingBackend>(
    panel: &DrawingArea<DB, Shift>,
    center: (i32, i32),
    outer_radius: f64,
    ring_width: f64,
    ring_gap: f64,
    snapshot: &Snapshot,
    total: u64,
) -> SvgOverlay
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    let (x_range, y_range) = panel.get_pixel_range();
    let center_abs = (x_range.start + center.0, y_range.start + center.1);
    SvgOverlay::Depth(DepthOverlay {
        center: center_abs,
        rings: vec![
            RingOverlay {
                outer_radius,
                inner_radius: (outer_radius - ring_width).max(0.0),
                value: snapshot.layer_coverage.pathway,
                total,
                color: PATHWAY_COLOR,
                background: REMAINING_COLOR,
            },
            RingOverlay {
                outer_radius: outer_radius - ring_width - ring_gap,
                inner_radius: (outer_radius - 2.0 * ring_width - ring_gap).max(0.0),
                value: snapshot.layer_coverage.superclass,
                total,
                color: SUPERCLASS_COLOR,
                background: REMAINING_COLOR,
            },
            RingOverlay {
                outer_radius: outer_radius - 2.0 * (ring_width + ring_gap),
                inner_radius: (outer_radius - 3.0 * ring_width - 2.0 * ring_gap).max(0.0),
                value: snapshot.layer_coverage.class,
                total,
                color: CLASS_COLOR,
                background: REMAINING_COLOR,
            },
        ],
        text_lines: Vec::new(),
    })
}

fn draw_depth_ring<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    center: (i32, i32),
    outer_radius: f64,
    ring_width: f64,
    value: u64,
    total: u64,
    color: RGBColor,
) -> io::Result<()>
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    let inner_radius = (outer_radius - ring_width).max(0.0);
    draw_annulus(area, center, outer_radius, inner_radius, REMAINING_COLOR)?;
    if value > 0 && total > 0 {
        let sweep = TAU * (value as f64 / total as f64);
        draw_annulus_sector(
            area,
            center,
            outer_radius,
            inner_radius,
            -FRAC_PI_2,
            -FRAC_PI_2 + sweep,
            color,
        )?;
    }
    Ok(())
}

fn draw_donut_chart<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    outer_radius: f64,
    inner_radius: f64,
    center: (i32, i32),
    slices: &[(u64, RGBColor)],
) -> io::Result<()>
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    let total: u64 = slices.iter().map(|(value, _)| *value).sum();
    if total == 0 {
        return Ok(());
    }

    let mut angle = -FRAC_PI_2;
    for &(value, color) in slices {
        if value == 0 {
            continue;
        }
        let sweep = TAU * (value as f64 / total as f64);
        draw_annulus_sector(
            area,
            center,
            outer_radius,
            inner_radius,
            angle,
            angle + sweep,
            color,
        )?;
        angle += sweep;
    }

    Ok(())
}

fn draw_annulus<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    center: (i32, i32),
    outer_radius: f64,
    inner_radius: f64,
    color: RGBColor,
) -> io::Result<()>
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    draw_annulus_sector(area, center, outer_radius, inner_radius, 0.0, TAU, color)
}

fn draw_annulus_sector<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    center: (i32, i32),
    outer_radius: f64,
    inner_radius: f64,
    start_angle: f64,
    end_angle: f64,
    color: RGBColor,
) -> io::Result<()>
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    let points = annulus_sector_points(center, outer_radius, inner_radius, start_angle, end_angle);
    area.draw(&Polygon::new(points, color.filled()))
        .map_err(plotters_error)?;
    Ok(())
}

fn annulus_sector_points(
    center: (i32, i32),
    outer_radius: f64,
    inner_radius: f64,
    start_angle: f64,
    end_angle: f64,
) -> Vec<(i32, i32)> {
    let sweep = (end_angle - start_angle).abs();
    let step_count = ((sweep / TAU) * 360.0).ceil() as usize;
    let outer_steps = step_count.max(8);
    let inner_steps = step_count.max(8);
    let mut points = Vec::with_capacity(outer_steps + inner_steps + 2);

    for step in 0..=outer_steps {
        let t = step as f64 / outer_steps as f64;
        let angle = start_angle + (end_angle - start_angle) * t;
        points.push(polar_to_point(center, outer_radius, angle));
    }
    for step in (0..=inner_steps).rev() {
        let t = step as f64 / inner_steps as f64;
        let angle = start_angle + (end_angle - start_angle) * t;
        points.push(polar_to_point(center, inner_radius, angle));
    }

    points
}

fn polar_to_point(center: (i32, i32), radius: f64, angle: f64) -> (i32, i32) {
    (
        center.0 + (radius * angle.cos()).round() as i32,
        center.1 + (radius * angle.sin()).round() as i32,
    )
}

fn draw_layer_panel<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    breakdown: &LayerBreakdown,
    color: RGBColor,
    top_n: usize,
) -> io::Result<()>
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    let panel = prepare_panel(area)?;
    panel
        .draw(&Text::new(
            format!("{} Breakdown", breakdown.title),
            (22, 28),
            TextStyle::from(("sans-serif", 24).into_font()).color(&TEXT),
        ))
        .map_err(plotters_error)?;
    panel
        .draw(&Text::new(
            format!(
                "{} label assignments | {} distinct labels",
                format_number(breakdown.assignment_total),
                breakdown.distinct_labels()
            ),
            (22, 56),
            TextStyle::from(("sans-serif", 17).into_font()).color(&MUTED),
        ))
        .map_err(plotters_error)?;

    if breakdown.labels.is_empty() {
        panel
            .draw(&Text::new(
                "No labels collected yet",
                (22, 110),
                TextStyle::from(("sans-serif", 19).into_font()).color(&TEXT),
            ))
            .map_err(plotters_error)?;
        return Ok(());
    }

    panel
        .draw(&Text::new(
            format!(
                "Top {} labels listed below; the strip encodes the full distribution.",
                breakdown.top_buckets(top_n).len()
            ),
            (22, 78),
            TextStyle::from(("sans-serif", 15).into_font()).color(&MUTED),
        ))
        .map_err(plotters_error)?;

    draw_distribution_strip(&panel, breakdown, color)?;

    let top_buckets = breakdown.top_buckets(top_n);
    let dims = panel.dim_in_pixel();
    let count_style = TextStyle::from(("sans-serif", 15).into_font()).color(&TEXT);
    let label_style = TextStyle::from(("sans-serif", 15).into_font()).color(&TEXT);
    let count_anchor_x = dims.0 as i32 - 24;
    let mut row_y = 162_i32;
    for (index, bucket) in top_buckets.iter().enumerate() {
        let bucket_color = ranked_color(color, index, top_buckets.len());
        let percent = format_percent(bucket.count, breakdown.assignment_total);
        let count_text = format!("{} | {}", percent, format_number(bucket.count));
        let (count_width, _) = panel
            .estimate_text_size(&count_text, &count_style)
            .map_err(plotters_error)?;
        let count_x = count_anchor_x - count_width as i32;
        let label_max_width = (count_x - 56).max(80) as u32;
        let label_text = truncate_to_width(&panel, &bucket.label, &label_style, label_max_width)?;
        panel
            .draw(&Rectangle::new(
                [(24, row_y), (38, row_y + 14)],
                bucket_color.filled(),
            ))
            .map_err(plotters_error)?;
        panel
            .draw(&Text::new(label_text, (46, row_y - 2), label_style.clone()))
            .map_err(plotters_error)?;
        panel
            .draw(&Text::new(
                count_text,
                (count_x, row_y - 2),
                count_style.clone(),
            ))
            .map_err(plotters_error)?;
        row_y += 26;
    }

    let tail_count = breakdown.tail_count(top_n);
    if tail_count > 0 {
        let tail_labels = breakdown
            .distinct_labels()
            .saturating_sub(top_buckets.len());
        panel
            .draw(&Text::new(
                format!(
                    "Tail: {} labels | {} | {}",
                    tail_labels,
                    format_percent(tail_count, breakdown.assignment_total),
                    format_number(tail_count)
                ),
                (24, row_y + 10),
                TextStyle::from(("sans-serif", 15).into_font()).color(&MUTED),
            ))
            .map_err(plotters_error)?;
    }

    Ok(())
}

fn draw_distribution_strip<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    breakdown: &LayerBreakdown,
    color: RGBColor,
) -> io::Result<()>
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    let left = 24_i32;
    let top = 100_i32;
    let right = area.dim_in_pixel().0 as i32 - 24;
    let bottom = 136_i32;
    let width = right - left;
    let total = breakdown.assignment_total.max(1);

    area.draw(&Rectangle::new(
        [(left, top), (right, bottom)],
        ShapeStyle::from(&PANEL_BORDER).stroke_width(1),
    ))
    .map_err(plotters_error)?;

    let mut cumulative = 0_u64;
    let last_index = breakdown.labels.len().saturating_sub(1);
    for (index, bucket) in breakdown.labels.iter().enumerate() {
        let start_x = left + ((cumulative as f64 / total as f64) * f64::from(width)).round() as i32;
        cumulative += bucket.count;
        let end_x = if index == last_index {
            right
        } else {
            left + ((cumulative as f64 / total as f64) * f64::from(width)).round() as i32
        };
        if end_x <= start_x {
            continue;
        }
        area.draw(&Rectangle::new(
            [(start_x, top), (end_x, bottom)],
            ranked_color(color, index, breakdown.labels.len()).filled(),
        ))
        .map_err(plotters_error)?;
    }

    Ok(())
}

fn draw_legend_entry<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    origin: (i32, i32),
    color: RGBColor,
    text: String,
) -> io::Result<()>
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    area.draw(&Rectangle::new(
        [origin, (origin.0 + 14, origin.1 + 14)],
        color.filled(),
    ))
    .map_err(plotters_error)?;
    area.draw(&Text::new(
        text,
        (origin.0 + 22, origin.1 - 2),
        TextStyle::from(("sans-serif", 15).into_font()).color(&TEXT),
    ))
    .map_err(plotters_error)?;
    Ok(())
}

fn dashboard_height(snapshot: &Snapshot, top_n: usize) -> u32 {
    let max_rows = snapshot
        .layers
        .iter()
        .map(|layer| layer.top_buckets(top_n).len() as u32)
        .max()
        .unwrap_or(0);
    let has_tail = snapshot
        .layers
        .iter()
        .any(|layer| layer.tail_count(top_n) > 0);
    let tail_height = if has_tail { 34 } else { 0 };
    let layers_row_height = 188 + max_rows * 26 + tail_height + 26;
    HEADER_HEIGHT + METRICS_ROW_HEIGHT + layers_row_height.max(320)
}

fn truncate_to_width<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    label: &str,
    style: &TextStyle<'_>,
    max_width: u32,
) -> io::Result<String>
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    let (label_width, _) = area
        .estimate_text_size(label, style)
        .map_err(plotters_error)?;
    if label_width <= max_width {
        return Ok(label.to_string());
    }

    let mut truncated = label.to_string();
    while !truncated.is_empty() {
        truncated.pop();
        while truncated.ends_with(char::is_whitespace) {
            truncated.pop();
        }
        let candidate = format!("{truncated}…");
        let (candidate_width, _) = area
            .estimate_text_size(&candidate, style)
            .map_err(plotters_error)?;
        if candidate_width <= max_width {
            return Ok(candidate);
        }
    }

    Ok("…".to_string())
}

fn ranked_color(base: RGBColor, index: usize, total: usize) -> RGBAColor {
    let total = total.max(1);
    let fade = index as f64 / total as f64;
    let opacity = (0.92 - fade * 0.55).clamp(0.28, 0.92);
    base.mix(opacity)
}

fn prepare_panel<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
) -> io::Result<DrawingArea<DB, Shift>>
where
    DB::ErrorType: std::error::Error + Send + Sync + 'static,
{
    area.fill(&PANEL_BG).map_err(plotters_error)?;
    let (width, height) = area.dim_in_pixel();
    area.draw(&Rectangle::new(
        [(0, 0), (width as i32 - 1, height as i32 - 1)],
        ShapeStyle::from(&PANEL_BORDER).stroke_width(1),
    ))
    .map_err(plotters_error)?;
    Ok(area.clone())
}

fn inject_svg_overlays(output_path: &Path, overlays: &[SvgOverlay]) -> io::Result<()> {
    if overlays.is_empty() {
        return Ok(());
    }

    let mut svg = fs::read_to_string(output_path)?;
    let insertion_point = svg.rfind("</svg>").ok_or_else(|| {
        io::Error::other(format!(
            "missing closing </svg> in {}",
            output_path.display()
        ))
    })?;

    let mut markup = String::new();
    for overlay in overlays {
        markup.push_str(&render_svg_overlay(overlay));
    }
    svg.insert_str(insertion_point, &markup);
    fs::write(output_path, svg)
}

fn render_svg_overlay(overlay: &SvgOverlay) -> String {
    match overlay {
        SvgOverlay::Metric(overlay) => render_metric_overlay_svg(overlay),
        SvgOverlay::Depth(overlay) => render_depth_overlay_svg(overlay),
    }
}

fn render_metric_overlay_svg(overlay: &MetricOverlay) -> String {
    let mut markup = String::from(
        "\n<g data-role=\"metric-donut-overlay\" shape-rendering=\"geometricPrecision\">\n",
    );
    let total: u64 = overlay.slices.iter().map(|slice| slice.value).sum();
    let mut start_angle = -FRAC_PI_2;
    for slice in overlay.slices.iter().filter(|slice| slice.value > 0) {
        let sweep = slice.value as f64 / total as f64 * TAU;
        markup.push_str("  <path fill=\"");
        markup.push_str(&svg_color(slice.color));
        markup.push_str("\" d=\"");
        markup.push_str(&donut_slice_path(
            overlay.center,
            overlay.outer_radius,
            overlay.inner_radius,
            start_angle,
            start_angle + sweep,
        ));
        markup.push_str("\"/>\n");
        start_angle += sweep;
    }
    for line in &overlay.text_lines {
        markup.push_str(&render_svg_text_line(line));
    }
    markup.push_str("</g>\n");
    markup
}

fn render_depth_overlay_svg(overlay: &DepthOverlay) -> String {
    let mut markup = String::from(
        "\n<g data-role=\"taxonomy-depth-overlay\" shape-rendering=\"geometricPrecision\">\n",
    );
    for ring in &overlay.rings {
        markup.push_str("  <path fill=\"");
        markup.push_str(&svg_color(ring.background));
        markup.push_str("\" d=\"");
        markup.push_str(&full_ring_path(
            overlay.center,
            ring.outer_radius,
            ring.inner_radius,
            -FRAC_PI_2,
        ));
        markup.push_str("\"/>\n");
        if ring.value > 0 && ring.total > 0 {
            let sweep = ring.value as f64 / ring.total as f64 * TAU;
            markup.push_str("  <path fill=\"");
            markup.push_str(&svg_color(ring.color));
            markup.push_str("\" d=\"");
            markup.push_str(&donut_slice_path(
                overlay.center,
                ring.outer_radius,
                ring.inner_radius,
                -FRAC_PI_2,
                -FRAC_PI_2 + sweep,
            ));
            markup.push_str("\"/>\n");
        }
    }
    for line in &overlay.text_lines {
        markup.push_str(&render_svg_text_line(line));
    }
    markup.push_str("</g>\n");
    markup
}

fn render_svg_text_line(line: &OverlayTextLine) -> String {
    format!(
        "  <text x=\"{}\" y=\"{}\" dy=\"0.76em\" text-anchor=\"middle\" font-family=\"sans-serif\" font-size=\"{:.6}\" opacity=\"1\" fill=\"{}\">{}</text>\n",
        line.center_x,
        line.y,
        svg_font_size(line.font_size),
        svg_color(line.color),
        escape_svg_text(&line.text),
    )
}

fn donut_slice_path(
    center: (i32, i32),
    outer_radius: f64,
    inner_radius: f64,
    start_angle: f64,
    end_angle: f64,
) -> String {
    let sweep = end_angle - start_angle;
    if sweep.abs() >= TAU - 1e-6 {
        return full_ring_path(center, outer_radius, inner_radius, start_angle);
    }

    let outer_start = polar_point(center, outer_radius, start_angle);
    let outer_end = polar_point(center, outer_radius, end_angle);
    let inner_end = polar_point(center, inner_radius, end_angle);
    let inner_start = polar_point(center, inner_radius, start_angle);
    let large_arc = i32::from(sweep.abs() > std::f64::consts::PI);

    format!(
        "M {} A {:.3} {:.3} 0 {} 1 {} L {} A {:.3} {:.3} 0 {} 0 {} Z",
        svg_point(outer_start),
        outer_radius,
        outer_radius,
        large_arc,
        svg_point(outer_end),
        svg_point(inner_end),
        inner_radius,
        inner_radius,
        large_arc,
        svg_point(inner_start),
    )
}

fn full_ring_path(
    center: (i32, i32),
    outer_radius: f64,
    inner_radius: f64,
    start_angle: f64,
) -> String {
    let outer_start = polar_point(center, outer_radius, start_angle);
    let outer_mid = polar_point(center, outer_radius, start_angle + std::f64::consts::PI);
    let inner_start = polar_point(center, inner_radius, start_angle);
    let inner_mid = polar_point(center, inner_radius, start_angle + std::f64::consts::PI);

    format!(
        "M {} A {:.3} {:.3} 0 1 1 {} A {:.3} {:.3} 0 1 1 {} L {} A {:.3} {:.3} 0 1 0 {} A {:.3} {:.3} 0 1 0 {} Z",
        svg_point(outer_start),
        outer_radius,
        outer_radius,
        svg_point(outer_mid),
        outer_radius,
        outer_radius,
        svg_point(outer_start),
        svg_point(inner_start),
        inner_radius,
        inner_radius,
        svg_point(inner_mid),
        inner_radius,
        inner_radius,
        svg_point(inner_start),
    )
}

fn polar_point(center: (i32, i32), radius: f64, angle: f64) -> (f64, f64) {
    (
        f64::from(center.0) + radius * angle.cos(),
        f64::from(center.1) + radius * angle.sin(),
    )
}

fn svg_point(point: (f64, f64)) -> String {
    format!("{:.3} {:.3}", point.0, point.1)
}

fn svg_color(color: RGBColor) -> String {
    format!("#{:02X}{:02X}{:02X}", color.0, color.1, color.2)
}

fn svg_font_size(size: f64) -> f64 {
    size * (25.0 / 31.0)
}

fn escape_svg_text(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            _ => escaped.push(character),
        }
    }
    escaped
}

fn format_number(value: u64) -> String {
    let digits = value.to_string();
    let mut formatted = String::with_capacity(digits.len() + digits.len() / 3);
    for (idx, ch) in digits.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            formatted.push(',');
        }
        formatted.push(ch);
    }
    formatted.chars().rev().collect()
}

fn format_percent(numerator: u64, denominator: u64) -> String {
    if denominator == 0 {
        return "0.00%".to_string();
    }
    format!("{:.2}%", numerator as f64 * 100.0 / denominator as f64)
}

fn sanitize_identifier(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

#[allow(clippy::needless_pass_by_value)]
fn json_error(error: serde_json::Error) -> io::Error {
    io::Error::other(error.to_string())
}

#[allow(clippy::needless_pass_by_value)]
fn plotters_error<E>(error: DrawingAreaErrorKind<E>) -> io::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    io::Error::other(format!("{error:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

    #[test]
    fn finalizes_breakdowns_keep_all_labels_sorted() {
        let mut counts = HashMap::new();
        counts.insert("A".to_string(), 5);
        counts.insert("B".to_string(), 4);
        counts.insert("C".to_string(), 3);

        let breakdown = finalize_breakdown("Pathway", counts);
        assert_eq!(breakdown.assignment_total, 12);
        assert_eq!(
            breakdown.labels,
            vec![
                CountBucket {
                    label: "A".to_string(),
                    count: 5,
                },
                CountBucket {
                    label: "B".to_string(),
                    count: 4,
                },
                CountBucket {
                    label: "C".to_string(),
                    count: 3,
                },
            ]
        );
    }

    #[test]
    fn pubchem_total_hint_is_used_for_zenodo_mode() {
        let temp_dir = test_dir("pubchem-total-hint");
        let args = Args {
            output: temp_dir.join("progress.svg"),
            input: temp_dir.join("missing-input.gz"),
            pubchem_total: None,
            zenodo_doi: DEFAULT_ZENODO_DOI.to_string(),
            cache_dir: temp_dir.join("cache"),
            top_n: 5,
        };

        assert_eq!(
            resolve_pubchem_total(&args, Some(123_456)).expect("hinted total"),
            123_456
        );
    }

    #[test]
    fn count_input_rows_reads_plain_and_gzip_inputs() {
        let temp_dir = test_dir("input-rows");
        let plain_path = temp_dir.join("CID-SMILES.txt");
        let gzip_path = temp_dir.join("CID-SMILES.gz");
        let payload = "1\tCCO\ninvalid\n2\tCCC\nbad\tDDD\n3\t\n";

        fs::write(&plain_path, payload).expect("write plain input");

        let gzip_file = File::create(&gzip_path).expect("create gzip input");
        let mut encoder = GzEncoder::new(gzip_file, Compression::default());
        encoder
            .write_all(payload.as_bytes())
            .expect("write gzip payload");
        encoder.finish().expect("finish gzip payload");

        assert_eq!(count_input_rows(&plain_path).expect("count plain rows"), 3);
        assert_eq!(count_input_rows(&gzip_path).expect("count gzip rows"), 3);
    }

    #[test]
    fn accumulate_completed_dataset_reads_classified_and_empty_rows() {
        let temp_dir = test_dir("completed-dataset");
        let dataset_path = temp_dir.join("completed.jsonl.zst");
        let mut encoder = zstd::stream::write::Encoder::new(
            File::create(&dataset_path).expect("create dataset"),
            1,
        )
        .expect("create encoder");
        writeln!(
            encoder,
            "{}",
            serde_json::json!({
                "class_results": ["Alkaloids"],
                "superclass_results": ["Nitrogen compounds"],
                "pathway_results": ["Alkaloids"]
            })
        )
        .expect("write classified row");
        writeln!(
            encoder,
            "{}",
            serde_json::json!({
                "class_results": [],
                "superclass_results": [],
                "pathway_results": []
            })
        )
        .expect("write empty row");
        encoder.finish().expect("finish dataset");

        let accumulator =
            accumulate_completed_dataset(&dataset_path).expect("read completed dataset");
        let (classified, empty, coverage, layers) = accumulator.into_layers();
        assert_eq!(classified, 1);
        assert_eq!(empty, 1);
        assert_eq!(coverage.pathway, 1);
        assert_eq!(coverage.superclass, 1);
        assert_eq!(coverage.class, 1);
        assert_eq!(layers[0].assignment_total, 1);
        assert_eq!(layers[1].labels[0].label, "Nitrogen compounds");
        assert_eq!(layers[2].labels[0].label, "Alkaloids");
    }

    #[test]
    fn manifest_request_metrics_reads_optional_fields() {
        let manifest = Manifest {
            created_at: "2026-04-14T12:00:00Z".to_string(),
            output_filename: "completed.jsonl.zst".to_string(),
            successful_rows: 10,
            invalid_rows: 2,
            failed_rows: 1,
            pubchem_total: Some(20),
            total_requests: Some(14),
            successful_requests: Some(10),
            invalid_requests: Some(2),
            failed_requests: Some(2),
        };

        let request_metrics = manifest_request_metrics(&manifest).expect("request metrics");
        assert_eq!(request_metrics.total_requests, 14);
        assert_eq!(request_metrics.successful_requests, 10);
        assert_eq!(request_metrics.invalid_responses, 2);
        assert_eq!(request_metrics.unsuccessful_requests, 2);
    }

    #[test]
    fn renderer_emits_svg() {
        let temp_dir = test_dir("render");
        let output = temp_dir.join("progress.svg");
        let snapshot = Snapshot {
            source_label: "latest Zenodo snapshot (10.5281/zenodo.14040990)".to_string(),
            timestamp_label: Some("2026-04-14T12:00:00Z".to_string()),
            counts: StatusCounts {
                classified: 4,
                empty: 1,
                invalid: 1,
                failed: 0,
                pending: 4,
            },
            request_metrics: None,
            layer_coverage: LayerCoverage {
                pathway: 4,
                superclass: 3,
                class: 2,
            },
            layers: [
                LayerBreakdown {
                    title: "Pathway",
                    assignment_total: 4,
                    labels: vec![
                        CountBucket {
                            label: "A".to_string(),
                            count: 3,
                        },
                        CountBucket {
                            label: "B".to_string(),
                            count: 1,
                        },
                    ],
                },
                LayerBreakdown {
                    title: "Superclass",
                    assignment_total: 3,
                    labels: vec![CountBucket {
                        label: "B".to_string(),
                        count: 3,
                    }],
                },
                LayerBreakdown {
                    title: "Class",
                    assignment_total: 2,
                    labels: vec![CountBucket {
                        label: "C".to_string(),
                        count: 2,
                    }],
                },
            ],
        };

        render_snapshot(&snapshot, &output, 5).expect("render svg");
        let svg = fs::read_to_string(&output).expect("read svg");
        assert!(svg.contains("<svg"));
        assert!(svg.contains("Collected vs PubChem"));
        assert!(svg.contains("Terminal Row Outcomes"));
        assert!(svg.contains("Taxonomy Depth Coverage"));
        assert!(svg.contains("Class Breakdown"));
        assert!(svg.contains("data-role=\"metric-donut-overlay\""));
        assert!(svg.contains("data-role=\"taxonomy-depth-overlay\""));
    }

    #[test]
    fn renderer_emits_png() {
        let temp_dir = test_dir("render-png");
        let output = temp_dir.join("progress.png");
        let snapshot = Snapshot {
            source_label: "latest Zenodo snapshot (10.5281/zenodo.14040990)".to_string(),
            timestamp_label: Some("2026-04-14T12:00:00Z".to_string()),
            counts: StatusCounts {
                classified: 4,
                empty: 1,
                invalid: 1,
                failed: 0,
                pending: 4,
            },
            request_metrics: None,
            layer_coverage: LayerCoverage {
                pathway: 4,
                superclass: 3,
                class: 2,
            },
            layers: [
                LayerBreakdown {
                    title: "Pathway",
                    assignment_total: 4,
                    labels: vec![
                        CountBucket {
                            label: "A".to_string(),
                            count: 3,
                        },
                        CountBucket {
                            label: "B".to_string(),
                            count: 1,
                        },
                    ],
                },
                LayerBreakdown {
                    title: "Superclass",
                    assignment_total: 3,
                    labels: vec![CountBucket {
                        label: "B".to_string(),
                        count: 3,
                    }],
                },
                LayerBreakdown {
                    title: "Class",
                    assignment_total: 2,
                    labels: vec![CountBucket {
                        label: "C".to_string(),
                        count: 2,
                    }],
                },
            ],
        };

        render_snapshot(&snapshot, &output, 5).expect("render png");
        let metadata = fs::metadata(&output).expect("stat png");
        assert!(metadata.len() > 10_000, "png output looked too small");
    }

    fn test_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "npc-progress-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create test dir");
        dir
    }
}
