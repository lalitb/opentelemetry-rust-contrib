#![allow(unsafe_attr_outside_unsafe)]

use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_uint};
use std::ptr;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

use geneva_uploader::client::{EncodedBatch, GenevaClient, GenevaClientConfig};
use geneva_uploader::AuthMethod;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;
use std::path::PathBuf;

/// Magic number for handle validation
const GENEVA_HANDLE_MAGIC: u64 = 0xFEED_BEEF;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4),
            )
            .thread_name("geneva-ffi-worker")
            .enable_time()
            .enable_io()
            .build()
            .expect("Failed to create Tokio runtime for Geneva FFI")
    })
}

trait ValidatedHandle {
    fn magic(&self) -> u64;
    fn set_magic(&mut self, magic: u64);
}

unsafe fn validate_handle<T: ValidatedHandle>(handle: *const T) -> GenevaError {
    if handle.is_null() {
        return GenevaError::NullPointer;
    }
    let h = match unsafe { handle.as_ref() } {
        Some(h) => h,
        None => return GenevaError::NullPointer,
    };
    if h.magic() != GENEVA_HANDLE_MAGIC {
        return GenevaError::InvalidData;
    }
    GenevaError::Success
}

unsafe fn clear_handle_magic<T: ValidatedHandle>(handle: *mut T) {
    if !handle.is_null() {
        if let Some(h) = unsafe { handle.as_mut() } {
            h.set_magic(0);
        }
    }
}

// New opaque handles per v2 API
pub struct GenevaClientHandle {
    magic: u64,
    client: Arc<GenevaClient>,
}
impl ValidatedHandle for GenevaClientHandle {
    fn magic(&self) -> u64 { self.magic }
    fn set_magic(&mut self, magic: u64) { self.magic = magic; }
}

pub struct GenevaBatchList {
    magic: u64,
    batches: Vec<EncodedBatch>,
}
impl ValidatedHandle for GenevaBatchList {
    fn magic(&self) -> u64 { self.magic }
    fn set_magic(&mut self, magic: u64) { self.magic = magic; }
}

pub struct GenevaBatch {
    magic: u64,
    batch: EncodedBatch,
}
impl ValidatedHandle for GenevaBatch {
    fn magic(&self) -> u64 { self.magic }
    fn set_magic(&mut self, magic: u64) { self.magic = magic; }
}

// Auth config structures
#[repr(C)]
#[derive(Copy, Clone)]
pub struct GenevaCertAuthConfig {
    pub cert_path: *const c_char,
    pub cert_password: *const c_char,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct GenevaMSIAuthConfig {
    pub objid: *const c_char,
}

#[repr(C)]
pub union GenevaAuthConfig {
    pub msi: GenevaMSIAuthConfig,
    pub cert: GenevaCertAuthConfig,
}

#[repr(C)]
pub struct GenevaConfig {
    pub endpoint: *const c_char,
    pub environment: *const c_char,
    pub account: *const c_char,
    pub namespace_name: *const c_char,
    pub region: *const c_char,
    pub config_major_version: c_uint,
    pub auth_method: c_int, // 0 = ManagedIdentity, 1 = Certificate
    pub tenant: *const c_char,
    pub role_name: *const c_char,
    pub role_instance: *const c_char,
    pub auth: GenevaAuthConfig,
}

#[repr(C)]
#[derive(PartialEq)]
pub enum GenevaError {
    Success = 0,
    InvalidConfig = 1,
    InitializationFailed = 2,
    UploadFailed = 3,
    InvalidData = 4,
    InternalError = 5,

    NullPointer = 100,
    EmptyInput = 101,
    DecodeFailed = 102,
    IndexOutOfRange = 103,

    InvalidAuthMethod = 110,
    InvalidCertConfig = 111,

    MissingEndpoint = 130,
    MissingEnvironment = 131,
    MissingAccount = 132,
    MissingNamespace = 133,
    MissingRegion = 134,
    MissingTenant = 135,
    MissingRoleName = 136,
    MissingRoleInstance = 137,
}

unsafe fn c_str_to_string(ptr: *const c_char, field_name: &str) -> Result<String, String> {
    if ptr.is_null() {
        return Err(format!("Field '{field_name}' is null"));
    }
    match unsafe { CStr::from_ptr(ptr) }.to_str() {
        Ok(s) => Ok(s.to_string()),
        Err(_) => Err(format!("Invalid UTF-8 in field '{field_name}'")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn geneva_client_new(
    config: *const GenevaConfig,
    out_handle: *mut *mut GenevaClientHandle,
) -> GenevaError {
    if config.is_null() || out_handle.is_null() {
        return GenevaError::NullPointer;
    }
    unsafe { *out_handle = ptr::null_mut() };

    let cfg = match unsafe { config.as_ref() } {
        Some(c) => c,
        None => return GenevaError::NullPointer,
    };

    if cfg.endpoint.is_null() { return GenevaError::MissingEndpoint; }
    if cfg.environment.is_null() { return GenevaError::MissingEnvironment; }
    if cfg.account.is_null() { return GenevaError::MissingAccount; }
    if cfg.namespace_name.is_null() { return GenevaError::MissingNamespace; }
    if cfg.region.is_null() { return GenevaError::MissingRegion; }
    if cfg.tenant.is_null() { return GenevaError::MissingTenant; }
    if cfg.role_name.is_null() { return GenevaError::MissingRoleName; }
    if cfg.role_instance.is_null() { return GenevaError::MissingRoleInstance; }

    let endpoint = match unsafe { c_str_to_string(cfg.endpoint, "endpoint") } { Ok(s) => s, Err(_) => return GenevaError::InvalidConfig };
    let environment = match unsafe { c_str_to_string(cfg.environment, "environment") } { Ok(s) => s, Err(_) => return GenevaError::InvalidConfig };
    let account = match unsafe { c_str_to_string(cfg.account, "account") } { Ok(s) => s, Err(_) => return GenevaError::InvalidConfig };
    let namespace = match unsafe { c_str_to_string(cfg.namespace_name, "namespace_name") } { Ok(s) => s, Err(_) => return GenevaError::InvalidConfig };
    let region = match unsafe { c_str_to_string(cfg.region, "region") } { Ok(s) => s, Err(_) => return GenevaError::InvalidConfig };
    let tenant = match unsafe { c_str_to_string(cfg.tenant, "tenant") } { Ok(s) => s, Err(_) => return GenevaError::InvalidConfig };
    let role_name = match unsafe { c_str_to_string(cfg.role_name, "role_name") } { Ok(s) => s, Err(_) => return GenevaError::InvalidConfig };
    let role_instance = match unsafe { c_str_to_string(cfg.role_instance, "role_instance") } { Ok(s) => s, Err(_) => return GenevaError::InvalidConfig };

    let auth_method = match cfg.auth_method {
        0 => AuthMethod::ManagedIdentity,
        1 => {
            let cert = unsafe { cfg.auth.cert };
            if cert.cert_path.is_null() || cert.cert_password.is_null() {
                return GenevaError::InvalidCertConfig;
            }
            let cert_path = match unsafe { c_str_to_string(cert.cert_path, "cert_path") } {
                Ok(s) => PathBuf::from(s),
                Err(_) => return GenevaError::InvalidConfig,
            };
            let cert_password = match unsafe { c_str_to_string(cert.cert_password, "cert_password") } {
                Ok(s) => s,
                Err(_) => return GenevaError::InvalidConfig,
            };
            AuthMethod::Certificate { path: cert_path, password: cert_password }
        }
        _ => return GenevaError::InvalidAuthMethod,
    };

    let geneva_config = GenevaClientConfig {
        endpoint,
        environment,
        account,
        namespace,
        region,
        config_major_version: cfg.config_major_version,
        auth_method,
        tenant,
        role_name,
        role_instance,
    };

    let client = match GenevaClient::new(geneva_config) {
        Ok(c) => Arc::new(c),
        Err(_) => return GenevaError::InitializationFailed,
        // map any internal error to InitializationFailed to avoid leaking strings over FFI
    };

    let handle = GenevaClientHandle { magic: GENEVA_HANDLE_MAGIC, client };
    unsafe { *out_handle = Box::into_raw(Box::new(handle)) };
    GenevaError::Success
}

#[no_mangle]
pub unsafe extern "C" fn geneva_client_free(handle: *mut GenevaClientHandle) {
    if !handle.is_null() {
        unsafe { clear_handle_magic(handle) };
        let _ = unsafe { Box::from_raw(handle) };
    }
}

// v2: encode -> GenevaBatchList
#[no_mangle]
pub unsafe extern "C" fn geneva_encode_and_compress_logs_v2(
    handle: *mut GenevaClientHandle,
    data: *const u8,
    data_len: usize,
    out_list: *mut *mut GenevaBatchList,
) -> GenevaError {
    if out_list.is_null() { return GenevaError::NullPointer; }
    unsafe { *out_list = ptr::null_mut() };

    if handle.is_null() || data.is_null() { return GenevaError::NullPointer; }
    if data_len == 0 { return GenevaError::EmptyInput; }

    if unsafe { validate_handle(handle) } != GenevaError::Success {
        return GenevaError::InvalidData;
    }

    let handle_ref = unsafe { handle.as_ref().unwrap() };
    let data_slice = unsafe { std::slice::from_raw_parts(data, data_len) };

    let logs_data: ExportLogsServiceRequest = match Message::decode(data_slice) {
        Ok(v) => v,
        Err(_) => return GenevaError::DecodeFailed,
    };

    match handle_ref.client.encode_and_compress_logs(&logs_data.resource_logs) {
        Ok(batches) => {
            let list = GenevaBatchList { magic: GENEVA_HANDLE_MAGIC, batches };
            unsafe { *out_list = Box::into_raw(Box::new(list)) };
            GenevaError::Success
        }
        Err(_) => GenevaError::InternalError,
    }
}

#[no_mangle]
pub unsafe extern "C" fn geneva_batches_len(list: *const GenevaBatchList) -> usize {
    match unsafe { validate_handle(list) } {
        GenevaError::Success => unsafe { list.as_ref().unwrap().batches.len() },
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn geneva_batches_free(list: *mut GenevaBatchList) {
    if !list.is_null() {
        unsafe { clear_handle_magic(list) };
        let _ = unsafe { Box::from_raw(list) };
    }
}

// Upload by index
#[no_mangle]
pub unsafe extern "C" fn geneva_batch_upload_by_index_sync(
    handle: *mut GenevaClientHandle,
    list: *const GenevaBatchList,
    index: usize,
) -> GenevaError {
    if unsafe { validate_handle(handle) } != GenevaError::Success { return GenevaError::NullPointer; }
    if unsafe { validate_handle(list) } != GenevaError::Success { return GenevaError::NullPointer; }

    let handle_ref = unsafe { handle.as_ref().unwrap() };
    let list_ref = unsafe { list.as_ref().unwrap() };

    if index >= list_ref.batches.len() {
        return GenevaError::IndexOutOfRange;
    }

    let batch = list_ref.batches[index].clone();
    let client = handle_ref.client.clone();
    let res = runtime().block_on(async move { client.upload_batch(&batch).await });
    match res {
        Ok(()) => GenevaError::Success,
        Err(_) => GenevaError::UploadFailed,
    }
}

// Take a single batch (ownership transfer)
#[no_mangle]
pub unsafe extern "C" fn geneva_batch_take(
    list: *mut GenevaBatchList,
    index: usize,
    out_batch: *mut *mut GenevaBatch,
) -> GenevaError {
    if out_batch.is_null() { return GenevaError::NullPointer; }
    unsafe { *out_batch = ptr::null_mut() };

    if unsafe { validate_handle(list) } != GenevaError::Success {
        return GenevaError::NullPointer;
    }
    let list_mut = unsafe { list.as_mut().unwrap() };
    if index >= list_mut.batches.len() {
        return GenevaError::IndexOutOfRange;
    }

    // O(1) swap_remove
    let taken = list_mut.batches.swap_remove(index);
    let single = GenevaBatch { magic: GENEVA_HANDLE_MAGIC, batch: taken };
    unsafe { *out_batch = Box::into_raw(Box::new(single)) };
    GenevaError::Success
}

#[no_mangle]
pub unsafe extern "C" fn geneva_single_upload_sync(
    handle: *mut GenevaClientHandle,
    batch: *const GenevaBatch,
) -> GenevaError {
    if unsafe { validate_handle(handle) } != GenevaError::Success { return GenevaError::NullPointer; }
    if unsafe { validate_handle(batch) } != GenevaError::Success { return GenevaError::NullPointer; }

    let handle_ref = unsafe { handle.as_ref().unwrap() };
    let batch_ref = unsafe { batch.as_ref().unwrap() };
    let client = handle_ref.client.clone();
    let b = batch_ref.batch.clone();

    let res = runtime().block_on(async move { client.upload_batch(&b).await });
    match res {
        Ok(()) => GenevaError::Success,
        Err(_) => GenevaError::UploadFailed,
    }
}

#[no_mangle]
pub unsafe extern "C" fn geneva_single_free(batch: *mut GenevaBatch) {
    if !batch.is_null() {
        unsafe { clear_handle_magic(batch) };
        let _ = unsafe { Box::from_raw(batch) };
    }
}

// Serialization format: "GNVB" (4) + u16 version(1) + u16 reserved(0) +
// u32 ev_len + ev bytes +
// u32 schemas_len + schemas bytes +
// u64 start + u64 end +
// u32 data_len + data bytes
#[no_mangle]
pub unsafe extern "C" fn geneva_batch_serialize(
    batch: *const GenevaBatch,
    out_buf: *mut u8,
    inout_len: *mut usize,
) -> GenevaError {
    if unsafe { validate_handle(batch) } != GenevaError::Success {
        return GenevaError::NullPointer;
    }
    if inout_len.is_null() {
        return GenevaError::NullPointer;
    }

    let b = unsafe { batch.as_ref().unwrap() };
    let ev = b.batch.event_name.as_bytes();
    let sc = b.batch.metadata.schema_ids.as_bytes();
    let data = b.batch.data.as_slice();
    let needed = 4 + 2 + 2 + 4 + ev.len() + 4 + sc.len() + 8 + 8 + 4 + data.len();

    // Size probe
    let len_ref = unsafe { inout_len.as_mut().unwrap() };
    if out_buf.is_null() || *len_ref < needed {
        *len_ref = needed;
        return GenevaError::Success;
    }

    // Copy
    let out = unsafe { std::slice::from_raw_parts_mut(out_buf, *len_ref) };
    let mut off = 0usize;
    // "GNVB"
    out[off..off + 4].copy_from_slice(b"GNVB");
    off += 4;
    // version=1, reserved=0 (little-endian)
    out[off..off + 2].copy_from_slice(&1u16.to_le_bytes());
    off += 2;
    out[off..off + 2].copy_from_slice(&0u16.to_le_bytes());
    off += 2;
    // event_name
    out[off..off + 4].copy_from_slice(&(ev.len() as u32).to_le_bytes());
    off += 4;
    out[off..off + ev.len()].copy_from_slice(ev);
    off += ev.len();
    // schema_ids
    out[off..off + 4].copy_from_slice(&(sc.len() as u32).to_le_bytes());
    off += 4;
    out[off..off + sc.len()].copy_from_slice(sc);
    off += sc.len();
    // times
    out[off..off + 8].copy_from_slice(&b.batch.metadata.start_time.to_le_bytes());
    off += 8;
    out[off..off + 8].copy_from_slice(&b.batch.metadata.end_time.to_le_bytes());
    off += 8;
    // data
    out[off..off + 4].copy_from_slice(&(data.len() as u32).to_le_bytes());
    off += 4;
    out[off..off + data.len()].copy_from_slice(data);
    off += data.len();

    // Final size must match needed; caller passed sufficient buffer
    *len_ref = off;
    GenevaError::Success
}

#[no_mangle]
pub unsafe extern "C" fn geneva_upload_serialized_batch_sync(
    handle: *mut GenevaClientHandle,
    blob_ptr: *const u8,
    blob_len: usize,
) -> GenevaError {
    if unsafe { validate_handle(handle) } != GenevaError::Success {
        return GenevaError::NullPointer;
    }
    if blob_ptr.is_null() {
        return GenevaError::NullPointer;
    }
    if blob_len < 4 + 2 + 2 + 4 + 4 + 8 + 8 + 4 {
        return GenevaError::InvalidData;
    }

    let blob = unsafe { std::slice::from_raw_parts(blob_ptr, blob_len) };
    let mut off = 0usize;
    // magic
    if &blob[off..off + 4] != b"GNVB" {
        return GenevaError::InvalidData;
    }
    off += 4;
    // version
    let ver = u16::from_le_bytes([blob[off], blob[off + 1]]);
    off += 2;
    let _reserved = u16::from_le_bytes([blob[off], blob[off + 1]]);
    off += 2;
    if ver != 1 {
        return GenevaError::InvalidData;
    }

    // event_name
    if off + 4 > blob_len { return GenevaError::InvalidData; }
    let ev_len = u32::from_le_bytes([blob[off], blob[off + 1], blob[off + 2], blob[off + 3]]) as usize;
    off += 4;
    if off + ev_len > blob_len { return GenevaError::InvalidData; }
    let event_name = match std::str::from_utf8(&blob[off..off + ev_len]) {
        Ok(s) => s.to_string(),
        Err(_) => return GenevaError::InvalidData,
    };
    off += ev_len;

    // schema_ids
    if off + 4 > blob_len { return GenevaError::InvalidData; }
    let sc_len = u32::from_le_bytes([blob[off], blob[off + 1], blob[off + 2], blob[off + 3]]) as usize;
    off += 4;
    if off + sc_len > blob_len { return GenevaError::InvalidData; }
    let schema_ids = match std::str::from_utf8(&blob[off..off + sc_len]) {
        Ok(s) => s.to_string(),
        Err(_) => return GenevaError::InvalidData,
    };
    off += sc_len;

    // times
    if off + 8 + 8 > blob_len { return GenevaError::InvalidData; }
    let mut sbytes = [0u8; 8];
    sbytes.copy_from_slice(&blob[off..off + 8]);
    let start_time = u64::from_le_bytes(sbytes);
    off += 8;
    sbytes.copy_from_slice(&blob[off..off + 8]);
    let end_time = u64::from_le_bytes(sbytes);
    off += 8;

    // data
    if off + 4 > blob_len { return GenevaError::InvalidData; }
    let data_len = u32::from_le_bytes([blob[off], blob[off + 1], blob[off + 2], blob[off + 3]]) as usize;
    off += 4;
    if off + data_len > blob_len { return GenevaError::InvalidData; }
    let data = blob[off..off + data_len].to_vec();
    // off += data_len; // not needed further

    // Rebuild EncodedBatch
    let metadata = geneva_uploader::BatchMetadata {
        start_time,
        end_time,
        schema_ids,
    };
    let batch = EncodedBatch {
        event_name,
        data,
        metadata,
    };

    // Upload
    let client = unsafe { handle.as_ref().unwrap().client.clone() };
    let res = runtime().block_on(async move { client.upload_batch(&batch).await });
    match res {
        Ok(()) => GenevaError::Success,
        Err(_) => GenevaError::UploadFailed,
    }
}

#[cfg(all(test, feature = "mock_auth"))]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;
    use std::ffi::CString;

    fn generate_mock_jwt_and_expiry(endpoint: &str, ttl_secs: i64) -> (String, String) {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        use chrono::{Duration, Utc};

        let header = r#"{"alg":"none","typ":"JWT"}"#;
        let exp = Utc::now() + Duration::seconds(ttl_secs);
        let payload = format!(r#"{{"Endpoint":"{endpoint}","exp":{}}}"#, exp.timestamp());

        let header_b64 = URL_SAFE_NO_PAD.encode(header.as_bytes());
        let payload_b64 = URL_SAFE_NO_PAD.encode(payload.as_bytes());
        let token = format!("{}.{}.{sig}", header_b64, payload_b64, sig = "dummy");

        (token, exp.to_rfc3339())
    }

    #[test]
    fn test_geneva_client_new_with_null_config() {
        unsafe {
            let mut out: *mut GenevaClientHandle = std::ptr::null_mut();
            let rc = geneva_client_new(std::ptr::null(), &mut out);
            assert_eq!(rc as u32, GenevaError::NullPointer as u32);
            assert!(out.is_null());
        }
    }

    #[test]
    fn test_batches_len_with_null() {
        unsafe {
            let n = geneva_batches_len(ptr::null());
            assert_eq!(n, 0);
        }
    }

    #[test]
    fn test_batches_free_with_null() {
        unsafe { geneva_batches_free(ptr::null_mut()); }
    }

    // Integration: encode -> upload first by index using MockAuth + MockServer
    #[test]
    fn test_encode_and_upload_by_index_mock_server() {
        use otlp_builder::builder::build_otlp_logs_minimal;
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = runtime().block_on(async { MockServer::start().await });
        let ingestion_endpoint = mock_server.uri();
        let (auth_token, auth_token_expiry) =
            generate_mock_jwt_and_expiry(&ingestion_endpoint, 24 * 3600);

        runtime().block_on(async {
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                    r#"{{
                        "IngestionGatewayInfo": {{
                            "Endpoint": "{ingestion_endpoint}",
                            "AuthToken": "{auth_token}",
                            "AuthTokenExpiryTime": "{auth_token_expiry}"
                        }},
                        "StorageAccountKeys": [{{
                            "AccountMonikerName": "testdiagaccount",
                            "AccountGroupName": "testgroup",
                            "IsPrimaryMoniker": true
                        }}],
                        "TagId": "test"
                    }}"#
                )))
                .mount(&mock_server)
                .await;

            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(202).set_body_string(r#"{"ticket":"accepted"}"#))
                .mount(&mock_server)
                .await;
        });

        // Build client with MockAuth (feature in dependency crate)
        let cfg = GenevaClientConfig {
            endpoint: mock_server.uri(),
            environment: "test".to_string(),
            account: "test".to_string(),
            namespace: "testns".to_string(),
            region: "testregion".to_string(),
            config_major_version: 1,
            auth_method: AuthMethod::MockAuth,
            tenant: "testtenant".to_string(),
            role_name: "testrole".to_string(),
            role_instance: "testinstance".to_string(),
        };
        let client = GenevaClient::new(cfg).expect("client");

        let mut handle_box = Box::new(GenevaClientHandle { magic: GENEVA_HANDLE_MAGIC, client: Arc::new(client) });
        let handle = &mut *handle_box as *mut GenevaClientHandle;

        let bytes = build_otlp_logs_minimal("TestEvent", "hello-world", Some(("rk", "rv")));

        let mut list_ptr: *mut GenevaBatchList = std::ptr::null_mut();
        let rc = unsafe { geneva_encode_and_compress_logs_v2(handle, bytes.as_ptr(), bytes.len(), &mut list_ptr) };
        assert_eq!(rc as u32, GenevaError::Success as u32);
        assert!(!list_ptr.is_null());

        let len = unsafe { geneva_batches_len(list_ptr) };
        assert!(len >= 1);

        let _ = unsafe { geneva_batch_upload_by_index_sync(handle, list_ptr as *const _, 0) };

        unsafe { geneva_batches_free(list_ptr) };
        let raw = Box::into_raw(handle_box);
        unsafe { geneva_client_free(raw) };
        drop(mock_server);
    }

    // Encode -> take single -> serialize -> upload from blob
    #[test]
    fn test_take_serialize_and_upload_blob() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = runtime().block_on(async { MockServer::start().await });
        let ingestion_endpoint = mock_server.uri();
        let (auth_token, auth_token_expiry) =
            generate_mock_jwt_and_expiry(&ingestion_endpoint, 24 * 3600);

        runtime().block_on(async {
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                    r#"{{
                        "IngestionGatewayInfo": {{
                            "Endpoint": "{ingestion_endpoint}",
                            "AuthToken": "{auth_token}",
                            "AuthTokenExpiryTime": "{auth_token_expiry}"
                        }},
                        "StorageAccountKeys": [{{
                            "AccountMonikerName": "testdiagaccount",
                            "AccountGroupName": "testgroup",
                            "IsPrimaryMoniker": true
                        }}],
                        "TagId": "test"
                    }}"#
                )))
                .mount(&mock_server)
                .await;

            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(202).set_body_string(r#"{"ticket":"accepted"}"#))
                .mount(&mock_server)
                .await;
        });

        // Build client with MockAuth
        let cfg = GenevaClientConfig {
            endpoint: mock_server.uri(),
            environment: "test".to_string(),
            account: "test".to_string(),
            namespace: "testns".to_string(),
            region: "testregion".to_string(),
            config_major_version: 1,
            auth_method: AuthMethod::MockAuth,
            tenant: "testtenant".to_string(),
            role_name: "testrole".to_string(),
            role_instance: "testinstance".to_string(),
        };
        let client = GenevaClient::new(cfg).expect("client");

        let mut handle_box = Box::new(GenevaClientHandle { magic: GENEVA_HANDLE_MAGIC, client: Arc::new(client) });
        let handle = &mut *handle_box as *mut GenevaClientHandle;

        // Build request with two different events to force at least one batch
        let log1 = opentelemetry_proto::tonic::logs::v1::LogRecord {
            observed_time_unix_nano: 1_700_000_000_000_000_001,
            event_name: "EventA".to_string(),
            severity_number: 9,
            ..Default::default()
        };
        let scope_logs = opentelemetry_proto::tonic::logs::v1::ScopeLogs { log_records: vec![log1], ..Default::default() };
        let resource_logs = opentelemetry_proto::tonic::logs::v1::ResourceLogs { scope_logs: vec![scope_logs], ..Default::default() };
        let req = ExportLogsServiceRequest { resource_logs: vec![resource_logs] };
        let bytes = req.encode_to_vec();

        let mut list_ptr: *mut GenevaBatchList = std::ptr::null_mut();
        let rc = unsafe { geneva_encode_and_compress_logs_v2(handle, bytes.as_ptr(), bytes.len(), &mut list_ptr) };
        assert_eq!(rc as u32, GenevaError::Success as u32);
        let len = unsafe { geneva_batches_len(list_ptr) };
        assert!(len >= 1);

        // Take first batch
        let mut single_ptr: *mut GenevaBatch = std::ptr::null_mut();
        let rc = unsafe { geneva_batch_take(list_ptr, 0, &mut single_ptr) };
        assert_eq!(rc as u32, GenevaError::Success as u32);
        assert!(!single_ptr.is_null());

        // Serialize it
        let mut needed: usize = 0;
        let rc = unsafe { geneva_batch_serialize(single_ptr as *const _, std::ptr::null_mut(), &mut needed) };
        assert_eq!(rc as u32, GenevaError::Success as u32);
        assert!(needed > 0);

        let mut buf = vec![0u8; needed];
        let mut len_inout = needed;
        let rc = unsafe { geneva_batch_serialize(single_ptr as *const _, buf.as_mut_ptr(), &mut len_inout) };
        assert_eq!(rc as u32, GenevaError::Success as u32);
        assert_eq!(len_inout, needed);

        // Upload from blob
        let rc = unsafe { geneva_upload_serialized_batch_sync(handle, buf.as_ptr(), buf.len()) };
        assert_eq!(rc as u32, GenevaError::Success as u32);

        // Cleanup
        unsafe { geneva_single_free(single_ptr) };
        unsafe { geneva_batches_free(list_ptr) };
        let raw = Box::into_raw(handle_box);
        unsafe { geneva_client_free(raw) };
        drop(mock_server);
    }
}
