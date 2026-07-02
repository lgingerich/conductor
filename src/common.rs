use chrono::Utc;

/// Returns the current UTC time formatted as ISO 8601 basic format:
/// `YYYYMMDDTHHMMSS` (e.g. `20260702T154301`).
pub(crate) fn iso8601_now() -> String {
    Utc::now().format("%Y%m%dT%H%M%S").to_string()
}
