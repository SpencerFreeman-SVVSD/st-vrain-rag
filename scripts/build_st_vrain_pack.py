from __future__ import annotations

import argparse
import io
import logging
import re
import warnings
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from pypdf import PdfReader
from requests import exceptions as requests_exceptions
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning


REQUEST_TIMEOUT = 45
DEFAULT_COVERAGE_WINDOW_DAYS = 90
USER_AGENT = "StVrainKnowledgePackBot/1.0"
SVVSD_HOSTS = {"svvsd.org", "www.svvsd.org"}
SVVSD_SITEMAP_URL = "https://www.svvsd.org/wp-sitemap.xml"
SVVSD_FEED_URL = "https://www.svvsd.org/feed/"
SVVSD_ALERT_FEED_URL = "https://www.svvsd.org/alerts/feed/"
SVVSD_BOARD_TEMPLATE = (
    "https://www.svvsd.org/about/board-of-education/board-meetings/{year}-board-meetings/"
)
SVVSD_FINANCE_URL = (
    "https://www.svvsd.org/departments/financial-services/required-financial-transparency/"
)
CDE_PROFILE_URL = "https://www.cde.state.co.us/schoolview/explore/profile/0470"
CDE_FRAMEWORK_URL = "https://www.cde.state.co.us/schoolview/frameworks/official/0470"
OUTPUT_DEFAULT = Path("knowledge/st_vrain_knowledge_pack.md")
SPLIT_OUTPUT_DIR_DEFAULT = Path("knowledge/st_vrain_knowledge_pack")

PACK_SECTION_FILENAMES = {
    "District snapshot": "01_district_snapshot.md",
    "Latest news and alerts": "02_latest_news_and_alerts.md",
    "Schools": "03_schools.md",
    "Departments and programs": "04_departments_and_programs.md",
    "Board meetings and governance": "05_board_meetings_and_governance.md",
    "Financial transparency": "06_financial_transparency.md",
    "CDE accountability and profile": "07_cde_accountability_and_profile.md",
    "Source Index": "08_source_index.md",
}

NOISE_LINE_PATTERNS = (
    re.compile(r"^Skip to main content$", re.IGNORECASE),
    re.compile(r"^Search by District or School Name$", re.IGNORECASE),
    re.compile(r"^Close$", re.IGNORECASE),
    re.compile(r"^Submit$", re.IGNORECASE),
    re.compile(r"^Read more$", re.IGNORECASE),
    re.compile(r"^Read less$", re.IGNORECASE),
    re.compile(r"^Staff Login$", re.IGNORECASE),
    re.compile(r"^St\. Vrain Valley Schools Home Page$", re.IGNORECASE),
)

TITLE_SUFFIX_PATTERNS = (
    re.compile(r"\s*\|\s*St\. Vrain Valley Schools\s*$", re.IGNORECASE),
    re.compile(r"\s*-\s*St\. Vrain Valley Schools\s*$", re.IGNORECASE),
    re.compile(r"\s*\|\s*SchoolView.*$", re.IGNORECASE),
    re.compile(r"\s*-\s*SchoolView.*$", re.IGNORECASE),
)

MOJIBAKE_REPLACEMENTS = {
    "â€™": "’",
    "â€œ": "“",
    "â€": "”",
    "â€“": "–",
    "â€”": "—",
    "â€¢": "•",
    "Ã©": "é",
    "Ã¨": "è",
}

DISTRICT_SNAPSHOT_PRIORITY = [
    "https://www.svvsd.org/about/district-overview/",
    "https://www.svvsd.org/about/district-overview/by-the-numbers-a-strong-competitive-advantage/",
    "https://www.svvsd.org/about/district-overview/strategic-priorities/",
    "https://www.svvsd.org/about/excellence-in-st-vrain/",
    "https://www.svvsd.org/about/partnerships/",
    "https://www.svvsd.org/about/superintendent/",
]


@dataclass(slots=True)
class PageRecord:
    title: str
    url: str
    last_modified: str | None
    text: str
    html_links: list[str]
    document_links: list[str]


@dataclass(slots=True)
class FeedItem:
    title: str
    link: str
    published_at: datetime
    summary: str


@dataclass(slots=True)
class BoardMeeting:
    title: str
    date_text: str
    url: str
    detail_text: str | None = None


def load_district_timezone() -> timezone:
    try:
        return ZoneInfo("America/Denver")
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=-7), name="America/Denver")


DISTRICT_TZ = load_district_timezone()
disable_warnings(InsecureRequestWarning)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
logging.getLogger("pypdf").setLevel(logging.ERROR)


def now_in_district_timezone() -> datetime:
    return datetime.now(tz=DISTRICT_TZ)


def current_board_years(now: datetime | None = None) -> list[int]:
    current = now or now_in_district_timezone()
    return [current.year, current.year + 1]


def build_seed_source_urls(now: datetime | None = None) -> list[str]:
    years = current_board_years(now)
    return [
        SVVSD_SITEMAP_URL,
        SVVSD_FEED_URL,
        SVVSD_ALERT_FEED_URL,
        SVVSD_BOARD_TEMPLATE.format(year=years[0]),
        SVVSD_BOARD_TEMPLATE.format(year=years[1]),
        SVVSD_FINANCE_URL,
        CDE_PROFILE_URL,
        CDE_FRAMEWORK_URL,
    ]


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def fetch_response(session: requests.Session, url: str) -> requests.Response:
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
    except requests_exceptions.SSLError:
        # Some local Windows Python installs lack the CA chain needed for these public sites.
        response = session.get(url, timeout=REQUEST_TIMEOUT, verify=False)
    response.raise_for_status()
    return response


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def iter_children(element: ET.Element, name: str) -> Iterable[ET.Element]:
    for child in element:
        if local_name(child.tag) == name:
            yield child


def child_text(element: ET.Element, name: str) -> str | None:
    for child in iter_children(element, name):
        if child.text:
            return child.text.strip()
    return None


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    normalized_path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if parsed.scheme and parsed.netloc:
        rebuilt = f"{parsed.scheme}://{parsed.netloc}{normalized_path}"
        if rebuilt.endswith("/") or "." in normalized_path.rsplit("/", 1)[-1]:
            return rebuilt
        return f"{rebuilt}/"
    return url


def clean_title(title: str) -> str:
    value = unescape(" ".join(title.split())).strip()
    for pattern in TITLE_SUFFIX_PATTERNS:
        value = pattern.sub("", value).strip()
    return value


def normalize_whitespace(text: str) -> str:
    text = unescape(text).replace("\r", "")
    for broken, fixed in MOJIBAKE_REPLACEMENTS.items():
        text = text.replace(broken, fixed)
    lines: list[str] = []
    previous = ""
    blank_run = 0
    for raw_line in text.split("\n"):
        line = " ".join(raw_line.split()).strip()
        if not line:
            blank_run += 1
            if blank_run <= 1 and lines:
                lines.append("")
            continue
        blank_run = 0
        if any(pattern.match(line) for pattern in NOISE_LINE_PATTERNS):
            continue
        if line == previous:
            continue
        lines.append(line)
        previous = line
    return "\n".join(lines).strip()


def strip_html_fragment(fragment: str) -> str:
    soup = BeautifulSoup(fragment or "", "html.parser")
    for node in soup(["script", "style", "noscript"]):
        node.decompose()
    return normalize_whitespace(soup.get_text("\n", strip=True))


def iso_or_none(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
            DISTRICT_TZ
        ).isoformat()
    except ValueError:
        pass
    try:
        return parsedate_to_datetime(value).astimezone(DISTRICT_TZ).isoformat()
    except (TypeError, ValueError):
        return value


def extract_last_modified(
    soup: BeautifulSoup, response: requests.Response, known_last_modified: str | None
) -> str | None:
    if known_last_modified:
        return iso_or_none(known_last_modified)

    candidates = [
        response.headers.get("Last-Modified"),
        soup.find("meta", attrs={"property": "article:modified_time"}),
        soup.find("meta", attrs={"name": "last-modified"}),
    ]

    for candidate in candidates:
        if hasattr(candidate, "get"):
            content = candidate.get("content")
            normalized = iso_or_none(content)
        else:
            normalized = iso_or_none(candidate)
        if normalized:
            return normalized
    return None


def extract_canonical_url(soup: BeautifulSoup, fallback_url: str) -> str:
    link = soup.find("link", attrs={"rel": lambda value: value and "canonical" in value})
    if link and link.get("href"):
        return normalize_url(link["href"])
    return normalize_url(fallback_url)


def sanitize_soup(soup: BeautifulSoup) -> BeautifulSoup:
    for selector in (
        "script",
        "style",
        "noscript",
        "svg",
        "form",
        "nav",
        "footer",
        "header",
        "aside",
        ".menu",
        ".site-footer",
        ".site-header",
        ".screen-reader-text",
        ".wpforms-container",
        ".g-recaptcha",
        ".modal",
        ".breadcrumbs",
        ".search-form",
    ):
        for node in soup.select(selector):
            node.decompose()
    return soup


def select_content_root(soup: BeautifulSoup) -> BeautifulSoup:
    for selector in ("#maincontent", "main", ".pagecontent", "#content", "article", "body"):
        node = soup.select_one(selector)
        if node is not None:
            return node
    return soup


def extract_links(
    soup: BeautifulSoup, base_url: str, *, include_documents: bool
) -> tuple[list[str], list[str]]:
    html_links: list[str] = []
    document_links: list[str] = []
    seen_html: set[str] = set()
    seen_docs: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        absolute = normalize_url(urljoin(base_url, anchor["href"]))
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        if include_documents and absolute.lower().endswith(".pdf"):
            if absolute not in seen_docs:
                seen_docs.add(absolute)
                document_links.append(absolute)
            continue
        if absolute.lower().endswith(
            (".jpg", ".jpeg", ".png", ".gif", ".svg", ".zip", ".mp4", ".mp3")
        ):
            continue
        if absolute not in seen_html:
            seen_html.add(absolute)
            html_links.append(absolute)
    return html_links, document_links


def fetch_html_page(
    session: requests.Session, url: str, known_last_modified: str | None = None
) -> tuple[PageRecord, BeautifulSoup]:
    response = fetch_response(session, url)
    response.encoding = "utf-8"
    soup = BeautifulSoup(response.text, "html.parser")
    html_links, document_links = extract_links(soup, response.url, include_documents=True)
    canonical_url = extract_canonical_url(soup, response.url)
    title = ""
    heading = soup.find("h1")
    if heading:
        title = heading.get_text(" ", strip=True)
    if not title and soup.title:
        title = soup.title.get_text(" ", strip=True)
    clean = sanitize_soup(soup)
    content_root = select_content_root(clean)
    text = normalize_whitespace(content_root.get_text("\n", strip=True))
    text = specialized_page_text(canonical_url, text)
    record = PageRecord(
        title=clean_title(title or canonical_url),
        url=canonical_url,
        last_modified=extract_last_modified(soup, response, known_last_modified),
        text=text,
        html_links=html_links,
        document_links=document_links,
    )
    return record, soup


def fetch_pdf_text(session: requests.Session, url: str, max_pages: int = 5) -> str:
    response = fetch_response(session, url)
    reader = PdfReader(io.BytesIO(response.content))
    extracted_pages: list[str] = []
    for page in reader.pages[:max_pages]:
        extracted_pages.append(page.extract_text() or "")
    return normalize_whitespace("\n".join(extracted_pages))


def load_sitemap_urls(session: requests.Session, sitemap_url: str) -> dict[str, str | None]:
    response = fetch_response(session, sitemap_url)
    root = ET.fromstring(response.content)
    tag = local_name(root.tag)
    entries: dict[str, str | None] = {}

    if tag == "sitemapindex":
        for sitemap in iter_children(root, "sitemap"):
            loc = child_text(sitemap, "loc")
            if not loc:
                continue
            entries.update(load_sitemap_urls(session, loc))
        return entries

    if tag == "urlset":
        for url_entry in iter_children(root, "url"):
            loc = child_text(url_entry, "loc")
            if not loc:
                continue
            entries[normalize_url(loc)] = child_text(url_entry, "lastmod")
        return entries

    raise ValueError(f"Unsupported sitemap document: {sitemap_url}")


def is_same_svvsd_host(url: str) -> bool:
    return urlparse(url).netloc.lower() in SVVSD_HOSTS


def is_year_board_page(path: str) -> bool:
    return bool(re.search(r"/about/board-of-education/board-meetings/\d{4}-board-", path))


def should_exclude_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    blocked_substrings = (
        "/wp-content/",
        "/wp-admin/",
        "/staffmembers/",
        "/login/",
        "/staff-portal/",
        "/district-contact-form/",
        "/website-accessibility/",
        "/legal-notices/",
        "/title-ix/",
        "/es/",
        "/?blackhole",
        "/blackhole",
    )
    if parsed.query or parsed.fragment:
        return True
    if any(part in path for part in blocked_substrings):
        return True
    if path.endswith((".jpg", ".jpeg", ".png", ".gif", ".svg", ".mp4", ".mp3", ".zip")):
        return True
    if path.endswith("/feed/"):
        return True
    return False


def should_include_discovered_url(url: str, years: list[int]) -> bool:
    if not is_same_svvsd_host(url) or should_exclude_url(url):
        return False

    path = urlparse(url).path.lower()
    exact_major_pages = {
        "/about/excellence-in-st-vrain/",
        "/about/partnerships/",
        "/about/superintendent/",
        "/career-and-technical-education/",
        "/2024-bond/",
    }
    prefix_pages = (
        "/about/district-overview/",
        "/about/district-committees/",
        "/about/board-of-education/",
        "/schools/",
        "/school/",
        "/departments/",
        "/programs/",
    )

    if path in exact_major_pages:
        return True
    if path.startswith(prefix_pages):
        if "/about/board-of-education/board-of-education-contact-form/" in path:
            return False
        if is_year_board_page(path):
            match = re.search(r"/(\d{4})-board-", path)
            return bool(match and int(match.group(1)) in years)
        if "/board-meetings/" in path and re.search(r"/\d{4}-board-meeting-archive/", path):
            return False
        return True
    if "required-financial-transparency" in path:
        return True
    return False


def is_school_hub_url(url: str) -> bool:
    return urlparse(url).path.lower() == "/schools/"


def parse_feed_items(
    session: requests.Session, feed_url: str, coverage_window_days: int
) -> list[FeedItem]:
    response = fetch_response(session, feed_url)
    root = ET.fromstring(response.content)
    cutoff = now_in_district_timezone() - timedelta(days=coverage_window_days)
    items: list[FeedItem] = []

    channel = next((child for child in root if local_name(child.tag) == "channel"), None)
    if channel is None:
        return items

    for item in iter_children(channel, "item"):
        title = child_text(item, "title") or "Untitled"
        link = normalize_url(child_text(item, "link") or "")
        pub_date = child_text(item, "pubDate")
        if not link or not pub_date:
            continue
        published_at = parsedate_to_datetime(pub_date).astimezone(DISTRICT_TZ)
        if published_at < cutoff:
            continue
        summary = ""
        for child in item:
            if local_name(child.tag) == "encoded" and child.text:
                summary = child.text
                break
        if not summary:
            summary = child_text(item, "description") or ""
        items.append(
            FeedItem(
                title=clean_title(title),
                link=link,
                published_at=published_at,
                summary=strip_html_fragment(summary),
            )
        )
    return items


def order_discovered_urls(urls: Iterable[str], priority: list[str]) -> list[str]:
    unique_urls = list(dict.fromkeys(normalize_url(url) for url in urls))
    priority_map = {normalize_url(url): index for index, url in enumerate(priority)}
    return sorted(unique_urls, key=lambda url: (priority_map.get(url, len(priority_map) + 1), url))


def parse_board_index(board_record: PageRecord, soup: BeautifulSoup) -> list[BoardMeeting]:
    meetings: list[BoardMeeting] = []
    for block in soup.select("div.board-meeting"):
        title_node = block.select_one(".board-meeting__title a")
        date_node = block.select_one(".board-meeting__date")
        if not title_node or not date_node or not title_node.get("href"):
            continue
        meetings.append(
            BoardMeeting(
                title=clean_title(title_node.get_text(" ", strip=True)),
                date_text=normalize_whitespace(date_node.get_text(" ", strip=True)),
                url=normalize_url(title_node["href"]),
            )
        )

    if meetings:
        return meetings

    for heading in soup.find_all(["h3", "h4", "h5"]):
        anchor = heading.find("a", href=True)
        if not anchor:
            continue
        parent_text = normalize_whitespace(heading.parent.get_text("\n", strip=True))
        meetings.append(
            BoardMeeting(
                title=clean_title(anchor.get_text(" ", strip=True)),
                date_text=parent_text,
                url=normalize_url(anchor["href"]),
            )
        )
    return meetings


def meaningful_board_detail(text: str) -> str | None:
    if not text:
        return None
    filtered_lines = []
    for line in text.splitlines():
        if (
            "395 S. Pratt" in line
            or line in {"Twitter", "LinkedIn", "Vimeo"}
            or line.startswith("Digital Accessibility Statement")
        ):
            continue
        filtered_lines.append(line)
    cleaned = normalize_whitespace("\n".join(filtered_lines))
    if len(cleaned.split()) < 25:
        return None
    return cleaned


def trim_for_markdown(text: str, max_chars: int | None = None) -> str:
    cleaned = normalize_whitespace(text)
    if max_chars and len(cleaned) > max_chars:
        return cleaned[: max_chars - 3].rstrip() + "..."
    return cleaned


def nonempty_lines(text: str) -> list[str]:
    return [line.strip() for line in normalize_whitespace(text).splitlines() if line.strip()]


def is_board_role_line(line: str) -> bool:
    normalized = line.strip().upper()
    return normalized in {
        "PRESIDENT",
        "VICE PRESIDENT",
        "SECRETARY",
        "TREASURER",
        "ASSISTANT SECRETARY",
        "ASSISTANT TREASURER",
        "DIRECTOR",
    }


def board_role_label(line: str) -> str:
    return line.title() if line.isupper() else line


def clean_labeled_value(value: str) -> str:
    return value.lstrip(": ").strip()


def format_board_governance_text(text: str) -> str:
    lines = nonempty_lines(text)
    output: list[str] = []
    i = 0

    while i < len(lines):
        line = lines[i]
        if line == "Members":
            i += 1
            continue

        if is_board_role_line(line):
            if output and output[-1] != "":
                output.append("")
            role = board_role_label(line)
            name = lines[i + 1] if i + 1 < len(lines) else ""
            if name and not is_board_role_line(name):
                output.append(f"Member: {name}")
                output.append(f"Role: {role}")
                i += 2
            else:
                output.append(f"Role: {role}")
                i += 1
            continue

        if line.startswith("District:"):
            output.append(line)
            i += 1
            continue

        if line == "Contact:":
            contact_parts: list[str] = []
            j = i + 1
            while j < len(lines) and not is_board_role_line(lines[j]) and lines[j] != "About":
                if lines[j].startswith("District:"):
                    break
                contact_parts.append(lines[j])
                j += 1
            if contact_parts:
                output.append(f"Contact: {' | '.join(contact_parts)}")
            i = j
            continue

        if line == "About":
            about_parts: list[str] = []
            j = i + 1
            while j < len(lines) and not is_board_role_line(lines[j]):
                if lines[j] in {"Contact:", "Members"}:
                    break
                about_parts.append(lines[j])
                j += 1
            if about_parts:
                output.append("About: " + " ".join(about_parts))
            i = j
            continue

        i += 1

    return "\n".join(output).strip()


def format_cde_profile_text(text: str) -> str:
    lines = nonempty_lines(text)
    output: list[str] = []
    noise_lines = {
        "Directions",
        "View School List",
        "District Website",
        "District Profile",
        "A MESSAGE FROM:",
        "More Enrollment Info",
        "More Staff Info",
        "More Performance Rating Info",
        "More Attendance Info",
        "More Graduation Info",
        "More Dropout Info",
        "What do these ratings mean?",
        "Unified Improvement Plan (UIP)",
    }
    metric_labels = {
        "Enrollment",
        "Student-Teacher Ratio",
        "Attendance Rate",
        "4-Year Grad Rate",
        "Dropout Rate",
    }
    i = 0

    while i < len(lines):
        line = lines[i]
        if line in noise_lines or line == ":":
            i += 1
            continue

        if i == 0:
            output.append(f"District: {line}")
            i += 1
            continue

        if (
            re.fullmatch(r"[\d,]+", line)
            and i + 1 < len(lines)
            and lines[i + 1] in {"Total Students Served", "Schools in District"}
        ):
            output.append(f"{lines[i + 1]}: {line}")
            i += 2
            continue

        if "PRATT PARKWAY" in line and i + 1 < len(lines) and "CO " in lines[i + 1]:
            output.append(f"Address: {line}, {lines[i + 1]}")
            i += 2
            continue

        if line == "Superintendent":
            j = i + 1
            while j < len(lines) and lines[j] == ":":
                j += 1
            if j < len(lines):
                output.append(f"Superintendent: {lines[j]}")
            i = j + 1
            continue

        if line == "About St Vrain Valley RE1J (0470)":
            i += 1
            continue

        if line == "St Vrain Valley RE1J" and i + 1 < len(lines) and re.fullmatch(r"20\d{2}-20\d{2}", lines[i + 1]):
            i += 2
            continue

        if line.startswith("St. Vrain Valley Schools (SVVS)"):
            narrative = [line]
            j = i + 1
            while j < len(lines) and lines[j] not in metric_labels and lines[j] != "Final 2025 Performance Rating":
                if lines[j] not in noise_lines:
                    narrative.append(lines[j])
                j += 1
            output.append("District message: " + " ".join(narrative))
            i = j
            continue

        if line in metric_labels and i + 1 < len(lines):
            value = lines[i + 1]
            year = (
                lines[i + 2]
                if i + 2 < len(lines) and lines[i + 2].startswith("School Year:")
                else None
            )
            formatted = f"{line}: {value}"
            if year:
                formatted += f" ({year})"
            output.append(formatted)
            i += 3 if year else 2
            continue

        if line == "Final 2025 Performance Rating":
            j = i + 1
            rating_parts: list[str] = []
            while j < len(lines) and lines[j] not in metric_labels:
                if lines[j] not in noise_lines and not lines[j].startswith("More "):
                    rating_parts.append(lines[j])
                j += 1
            if rating_parts:
                output.append("Final 2025 Performance Rating: " + " ".join(rating_parts))
            i = j
            continue

        i += 1

    return "\n".join(output).strip()


def format_cde_framework_text(text: str) -> str:
    lines = nonempty_lines(text)
    output: list[str] = []
    resource_labels = {
        "Unified Improvement Plan (UIP)",
        "Accreditation Contract PDF",
        "Accreditation Contract Plain Text",
    }
    i = 0
    active_block_started = False

    while i < len(lines):
        line = lines[i]

        if i == 0:
            output.append(line)
            i += 1
            continue

        if i == 1:
            output.append(f"District: {line}")
            i += 1
            continue

        if "PRATT PARKWAY" in line and i + 1 < len(lines) and "CO " in lines[i + 1]:
            output.append(f"Address: {line}, {lines[i + 1]}")
            i += 2
            continue

        if line in {"County:", "Number of Schools:"} and i + 1 < len(lines):
            output.append(f"{line.rstrip(':')}: {lines[i + 1]}")
            i += 2
            continue

        if line in resource_labels:
            output.append(f"Resource: {line}")
            i += 1
            continue

        if line == "Selected Report Year" and not active_block_started:
            active_block_started = True
            year = ""
            j = i + 1
            while j < len(lines) and lines[j] == ":":
                j += 1
            if j < len(lines):
                year = clean_labeled_value(lines[j])
            output.append(f"Selected Report Year: {year}")
            i = j + 1
            continue

        if active_block_started and line in {"Rating", "Performance Watch Status", "Rating Source"}:
            j = i + 1
            while j < len(lines) and lines[j] == ":":
                j += 1
            if j < len(lines):
                output.append(f"{line}: {clean_labeled_value(lines[j])}")
            i = j + 1
            continue

        if active_block_started and line == "Selected Report Year":
            break

        i += 1

    return "\n".join(output).strip()


def specialized_page_text(url: str, text: str) -> str:
    normalized_url = normalize_url(url)
    if normalized_url == normalize_url("https://www.svvsd.org/about/board-of-education/"):
        return format_board_governance_text(text)
    if normalized_url == normalize_url(CDE_PROFILE_URL):
        return format_cde_profile_text(text)
    if normalized_url == normalize_url(CDE_FRAMEWORK_URL):
        return format_cde_framework_text(text)
    return text


def page_section_markdown(record: PageRecord, *, body_limit: int | None = None) -> str:
    lines = [f"### {record.title}", ""]
    lines.append(f"- Canonical URL: {record.url}")
    if record.last_modified:
        lines.append(f"- Last modified: {record.last_modified}")
    lines.append("")
    lines.append(trim_for_markdown(record.text, max_chars=body_limit) or "No substantive text extracted.")
    lines.append("")
    lines.append(f"Source: {record.url}")
    return "\n".join(lines).strip()


def feed_item_markdown(item: FeedItem) -> str:
    return "\n".join(
        [
            f"### {item.title}",
            "",
            f"- Published: {item.published_at.isoformat()}",
            f"- Source URL: {item.link}",
            "",
            trim_for_markdown(item.summary, max_chars=4000) or "No summary extracted.",
            "",
            f"Source: {item.link}",
        ]
    ).strip()


def board_meeting_markdown(meeting: BoardMeeting) -> str:
    lines = [
        f"### {meeting.title}",
        "",
        f"- Meeting date: {meeting.date_text}",
        f"- Detail URL: {meeting.url}",
        "",
    ]
    if meeting.detail_text:
        lines.append(trim_for_markdown(meeting.detail_text, max_chars=5000))
    else:
        lines.append("The linked detail page did not expose enough unique text to inline beyond the year index entry.")
    lines.extend(["", f"Source: {meeting.url}"])
    return "\n".join(lines).strip()


def pdf_section_markdown(title: str, url: str, text: str) -> str:
    body = trim_for_markdown(text, max_chars=9000) or "No PDF text extracted."
    return "\n".join([f"### {title}", "", f"- Document URL: {url}", "", body, "", f"Source: {url}"]).strip()


def render_frontmatter(generated_at: datetime, source_urls: list[str], coverage_window_days: int) -> str:
    lines = [
        "---",
        f"generated_at: {generated_at.isoformat()}",
        f"source_count: {len(source_urls)}",
        "source_urls:",
    ]
    for url in source_urls:
        lines.append(f"  - {url}")
    lines.append(f"coverage_window_days: {coverage_window_days}")
    lines.append("---")
    return "\n".join(lines)


def render_source_index(
    seed_sources: list[str],
    district_snapshot_pages: list[PageRecord],
    school_pages: list[PageRecord],
    department_pages: list[PageRecord],
    governance_pages: list[PageRecord],
    board_indexes: list[PageRecord],
    board_meetings: list[BoardMeeting],
    financial_pages: list[PageRecord],
    cde_pages: list[PageRecord],
    document_urls: list[str],
) -> str:
    lines = ["## Source Index", "", "### Seed sources", ""]
    for url in seed_sources:
        lines.append(f"- {url}")

    lines.extend(
        [
            "",
            "### Supporting coverage counts",
            "",
            f"- District snapshot pages: {len(district_snapshot_pages)}",
            f"- School pages: {len(school_pages)}",
            f"- Department and program pages: {len(department_pages)}",
            f"- Governance pages: {len(governance_pages)}",
            f"- Board year index pages: {len(board_indexes)}",
            f"- Board meeting entries: {len(board_meetings)}",
            f"- Financial transparency pages: {len(financial_pages)}",
            f"- CDE pages: {len(cde_pages)}",
            f"- Referenced documents: {len(document_urls)}",
        ]
    )

    if document_urls:
        lines.extend(["", "### Referenced documents", ""])
        for url in document_urls:
            lines.append(f"- {url}")

    return "\n".join(lines).strip()


def slugify_heading(title: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", title.lower()).strip("_")
    return slug or "section"


def section_output_filename(title: str, position: int) -> str:
    return PACK_SECTION_FILENAMES.get(title, f"{position:02d}_{slugify_heading(title)}.md")


def split_markdown_sections(markdown: str) -> tuple[str, list[tuple[str, str]]]:
    preamble_lines: list[str] = []
    sections: list[tuple[str, str]] = []
    current_title: str | None = None
    current_lines: list[str] = []

    for line in markdown.splitlines():
        if line.startswith("## "):
            if current_title is None:
                preamble_lines = current_lines
            else:
                sections.append((current_title, "\n".join(current_lines).strip()))
            current_title = line[3:].strip()
            current_lines = []
            continue
        current_lines.append(line)

    if current_title is None:
        return markdown.strip(), []

    sections.append((current_title, "\n".join(current_lines).strip()))
    return "\n".join(preamble_lines).strip(), sections


def render_split_pack_readme(
    preamble: str,
    section_entries: list[tuple[str, str]],
) -> str:
    lines = [preamble.strip(), "", "## Section files", ""]
    for filename, title in section_entries:
        lines.append(f"- [{title}]({filename})")
    return "\n".join(line for line in lines if line is not None).strip() + "\n"


def render_split_section_document(
    title: str,
    body: str,
    generated_at: datetime,
    coverage_window_days: int,
) -> str:
    lines = [
        "---",
        f"generated_at: {generated_at.isoformat()}",
        f"coverage_window_days: {coverage_window_days}",
        f"section_title: {title}",
        "---",
        "",
        f"# {title}",
        "",
        body.strip() or "No content generated for this section.",
    ]
    return "\n".join(lines).strip() + "\n"


def build_split_pack_documents(
    markdown: str,
    generated_at: datetime,
    coverage_window_days: int,
) -> dict[str, str]:
    preamble, sections = split_markdown_sections(markdown)
    documents: dict[str, str] = {}
    section_entries: list[tuple[str, str]] = []

    for position, (title, body) in enumerate(sections, start=1):
        filename = section_output_filename(title, position)
        section_entries.append((filename, title))
        documents[filename] = render_split_section_document(
            title=title,
            body=body,
            generated_at=generated_at,
            coverage_window_days=coverage_window_days,
        )

    documents["README.md"] = render_split_pack_readme(preamble, section_entries)
    return documents


def write_split_pack(
    output_dir: Path,
    markdown: str,
    generated_at: datetime,
    coverage_window_days: int,
) -> None:
    documents = build_split_pack_documents(markdown, generated_at, coverage_window_days)
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, content in documents.items():
        (output_dir / filename).write_text(content, encoding="utf-8")


def collect_financial_pages(
    session: requests.Session,
    discovered_urls: dict[str, str | None],
) -> tuple[list[PageRecord], list[tuple[str, str, str]]]:
    prefix = normalize_url(SVVSD_FINANCE_URL)
    finance_urls = [
        url
        for url in discovered_urls
        if url.startswith(prefix) and "cde-financial-transparency-website" not in url
    ]
    ordered = order_discovered_urls(finance_urls, [SVVSD_FINANCE_URL])
    pages: list[PageRecord] = []
    extracted_documents: list[tuple[str, str, str]] = []

    for url in ordered:
        record, _ = fetch_html_page(session, url, discovered_urls.get(url))
        if record.text:
            pages.append(record)
        for doc_url in record.document_links:
            if doc_url.startswith("https://www.svvsd.org/") and doc_url.lower().endswith(".pdf"):
                try:
                    extracted_documents.append(
                        (doc_url.rsplit("/", 1)[-1], doc_url, fetch_pdf_text(session, doc_url))
                    )
                except Exception:
                    continue
    return pages, extracted_documents[:2]


def collect_cde_pages(session: requests.Session) -> tuple[list[PageRecord], list[tuple[str, str, str]]]:
    pages: list[PageRecord] = []
    extracted_documents: list[tuple[str, str, str]] = []

    profile_record, _ = fetch_html_page(session, CDE_PROFILE_URL)
    pages.append(profile_record)

    framework_record, framework_soup = fetch_html_page(session, CDE_FRAMEWORK_URL)
    pages.append(framework_record)

    framework_document_links = []
    for link in framework_record.document_links:
        if "cedar2.cde.state.co.us" in link.lower() and link.lower().endswith(".pdf"):
            framework_document_links.append(link)
    current_year = current_board_years()[0] - 1
    preferred_pdf = next(
        (
            link
            for link in framework_document_links
            if f"DPF{current_year}" in link and "Official" in link
        ),
        framework_document_links[0] if framework_document_links else None,
    )
    if preferred_pdf:
        try:
            extracted_documents.append(
                (
                    "Latest official district performance framework PDF",
                    preferred_pdf,
                    fetch_pdf_text(session, preferred_pdf),
                )
            )
        except Exception:
            pass

    plain_text_link = None
    for anchor in framework_soup.find_all("a", href=True):
        href = normalize_url(urljoin(CDE_FRAMEWORK_URL, anchor["href"]))
        if "accreditationcontractplaintext" in href.lower():
            plain_text_link = href
            break
    if plain_text_link:
        try:
            plain_text_record, _ = fetch_html_page(session, plain_text_link)
            pages.append(plain_text_record)
        except Exception:
            pass

    return pages, extracted_documents[:2]


def build_markdown(
    output_path: Path,
    coverage_window_days: int,
    session: requests.Session,
    split_output_dir: Path | None = None,
) -> str:
    generated_at = now_in_district_timezone()
    seed_sources = build_seed_source_urls(generated_at)
    discovered_urls = load_sitemap_urls(session, SVVSD_SITEMAP_URL)
    years = current_board_years(generated_at)

    filtered_urls = {
        url: lastmod
        for url, lastmod in discovered_urls.items()
        if should_include_discovered_url(url, years)
    }

    district_snapshot_urls = [
        url
        for url in filtered_urls
        if url.startswith("https://www.svvsd.org/about/district-overview/")
        or url
        in {
            "https://www.svvsd.org/about/excellence-in-st-vrain/",
            "https://www.svvsd.org/about/partnerships/",
            "https://www.svvsd.org/about/superintendent/",
            "https://www.svvsd.org/about/district-committees/leadership-st-vrain/",
        }
    ]
    school_info_urls = [
        url
        for url in filtered_urls
        if urlparse(url).path.lower().startswith("/schools/")
        and not is_school_hub_url(url)
    ]
    school_urls = [url for url in filtered_urls if "/school/" in urlparse(url).path.lower()]
    department_urls = [
        url
        for url in filtered_urls
        if any(
            urlparse(url).path.lower().startswith(prefix)
            for prefix in ("/departments/", "/programs/", "/career-and-technical-education/")
        )
        and "required-financial-transparency" not in url.lower()
    ]
    governance_urls = [
        url
        for url in filtered_urls
        if url.startswith("https://www.svvsd.org/about/board-of-education/")
        and not is_year_board_page(urlparse(url).path.lower())
        and "/board-of-education-contact-form/" not in url
    ]

    district_snapshot_pages = []
    for url in order_discovered_urls(district_snapshot_urls, DISTRICT_SNAPSHOT_PRIORITY):
        record, _ = fetch_html_page(session, url, filtered_urls.get(url))
        if record.text:
            district_snapshot_pages.append(record)

    school_info_pages = []
    for url in sorted(school_info_urls):
        record, _ = fetch_html_page(session, url, filtered_urls.get(url))
        if record.text:
            school_info_pages.append(record)

    school_pages = []
    for url in sorted(school_urls):
        record, _ = fetch_html_page(session, url, filtered_urls.get(url))
        if record.text:
            school_pages.append(record)

    department_pages = []
    for url in sorted(department_urls):
        record, _ = fetch_html_page(session, url, filtered_urls.get(url))
        if record.text:
            department_pages.append(record)

    governance_pages = []
    for url in sorted(governance_urls):
        record, _ = fetch_html_page(session, url, filtered_urls.get(url))
        if record.text:
            governance_pages.append(record)

    news_items = parse_feed_items(session, SVVSD_FEED_URL, coverage_window_days)
    alert_items = parse_feed_items(session, SVVSD_ALERT_FEED_URL, coverage_window_days)

    board_index_pages: list[PageRecord] = []
    board_meetings: list[BoardMeeting] = []
    for year in years:
        board_url = SVVSD_BOARD_TEMPLATE.format(year=year)
        board_record, board_soup = fetch_html_page(session, board_url, filtered_urls.get(board_url))
        board_index_pages.append(board_record)
        for meeting in parse_board_index(board_record, board_soup):
            try:
                meeting_record, _ = fetch_html_page(session, meeting.url, filtered_urls.get(meeting.url))
                meeting.detail_text = meaningful_board_detail(meeting_record.text)
            except Exception:
                meeting.detail_text = None
            board_meetings.append(meeting)

    financial_pages, financial_documents = collect_financial_pages(session, filtered_urls)
    cde_pages, cde_documents = collect_cde_pages(session)

    document_urls = [url for _, url, _ in financial_documents]
    for page in cde_pages:
        for link in page.document_links:
            if link.lower().endswith(".pdf") and link not in document_urls:
                document_urls.append(link)

    sections = [
        render_frontmatter(generated_at, seed_sources, coverage_window_days),
        "",
        "# St. Vrain Knowledge Pack",
        "",
        "This generated pack is built from a small authoritative source set and published in both combined and split markdown formats so a RAG system can ingest GitHub-hosted documents instead of crawling hundreds of direct URLs.",
        "",
        "## District snapshot",
        "",
    ]
    sections.extend(page_section_markdown(page, body_limit=12000) + "\n" for page in district_snapshot_pages)

    sections.extend(["## Latest news and alerts", ""])
    sections.extend(["### District news", ""])
    if news_items:
        sections.extend(feed_item_markdown(item) + "\n" for item in news_items)
    else:
        sections.extend(["No recent district news items were found within the configured coverage window.", ""])

    sections.extend(["### District alerts", ""])
    if alert_items:
        sections.extend(feed_item_markdown(item) + "\n" for item in alert_items)
    else:
        sections.extend(["No recent alert items were found within the configured coverage window.", ""])

    sections.extend(["## Schools", ""])
    sections.extend(page_section_markdown(page, body_limit=12000) + "\n" for page in school_info_pages)
    sections.extend(page_section_markdown(page, body_limit=14000) + "\n" for page in school_pages)

    sections.extend(["## Departments and programs", ""])
    sections.extend(page_section_markdown(page, body_limit=14000) + "\n" for page in department_pages)

    sections.extend(["## Board meetings and governance", ""])
    sections.extend(page_section_markdown(page, body_limit=12000) + "\n" for page in governance_pages)
    sections.extend(page_section_markdown(page, body_limit=12000) + "\n" for page in board_index_pages)
    sections.extend(board_meeting_markdown(meeting) + "\n" for meeting in board_meetings)

    sections.extend(["## Financial transparency", ""])
    sections.extend(page_section_markdown(page, body_limit=12000) + "\n" for page in financial_pages)
    for title, url, text in financial_documents:
        sections.append(pdf_section_markdown(title, url, text))
        sections.append("")

    sections.extend(["## CDE accountability and profile", ""])
    sections.extend(page_section_markdown(page, body_limit=15000) + "\n" for page in cde_pages)
    for title, url, text in cde_documents:
        sections.append(pdf_section_markdown(title, url, text))
        sections.append("")

    sections.append(
        render_source_index(
            seed_sources=seed_sources,
            district_snapshot_pages=district_snapshot_pages,
            school_pages=school_info_pages + school_pages,
            department_pages=department_pages,
            governance_pages=governance_pages,
            board_indexes=board_index_pages,
            board_meetings=board_meetings,
            financial_pages=financial_pages,
            cde_pages=cde_pages,
            document_urls=document_urls,
        )
    )

    markdown = "\n".join(section for section in sections if section is not None).strip() + "\n"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")
    if split_output_dir is not None:
        write_split_pack(split_output_dir, markdown, generated_at, coverage_window_days)
    return markdown


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build combined and split markdown knowledge packs for St. Vrain Valley Schools."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_DEFAULT,
        help="Combined markdown output file path.",
    )
    parser.add_argument(
        "--split-output-dir",
        type=Path,
        default=SPLIT_OUTPUT_DIR_DEFAULT,
        help="Directory for split section markdown files.",
    )
    parser.add_argument(
        "--coverage-window-days",
        type=int,
        default=DEFAULT_COVERAGE_WINDOW_DAYS,
        help="How many recent days of feed items to include.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with build_session() as session:
        build_markdown(
            args.output,
            args.coverage_window_days,
            session,
            split_output_dir=args.split_output_dir,
        )


if __name__ == "__main__":
    main()
