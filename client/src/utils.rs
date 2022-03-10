use uuid::Uuid;

/// Generates a new random correlation ID for use in read requests.
///
/// You must use the same correlation ID for all read segment column and
/// read segment deletion requests of a single segment at a point in time,
/// or else the data returned might not be consistent.
/// Using the same correlation ID when reading different segments or the same
/// segment at different points in time (e.g. separated by hours) can result
/// in errors or inconsistent data.
/// Therefore, calling this function once each time you need to read a segment
/// is the best option.
pub fn new_correlation_id() -> String {
  Uuid::new_v4().to_string()
}
