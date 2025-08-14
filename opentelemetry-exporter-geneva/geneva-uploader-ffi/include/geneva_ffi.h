#ifndef GENEVA_FFI_H
#define GENEVA_FFI_H

#include <stdint.h>
#include <stdlib.h>
#include "geneva_errors.h"

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handles
typedef struct GenevaClientHandle GenevaClientHandle;
typedef struct GenevaBatchList    GenevaBatchList;   // list of encoded/compressed batches (chunks)
typedef struct GenevaBatch        GenevaBatch;       // single batch (owned)

// Authentication method constants
#define GENEVA_AUTH_MANAGED_IDENTITY 0
#define GENEVA_AUTH_CERTIFICATE 1

/* Configuration for certificate auth (valid only when auth_method == GENEVA_AUTH_CERTIFICATE) */
typedef struct {
    const char* cert_path;      /* Path to certificate file */
    const char* cert_password;  /* Certificate password */
} GenevaCertAuthConfig;

/* Configuration for managed identity auth (valid only when auth_method == GENEVA_AUTH_MANAGED_IDENTITY) */
typedef struct {
    const char* objid; /* Optional: Azure AD object ID as NUL-terminated GUID string
                          e.g. "00000000-0000-0000-0000-000000000000" */
} GenevaMSIAuthConfig;

/* Tagged union for auth-specific configuration.
   The active member is determined by 'auth_method' in GenevaConfig. */
typedef union {
    GenevaMSIAuthConfig msi;    /* Valid when auth_method == GENEVA_AUTH_MANAGED_IDENTITY */
    GenevaCertAuthConfig cert;  /* Valid when auth_method == GENEVA_AUTH_CERTIFICATE */
} GenevaAuthConfig;

/* Configuration structure for Geneva client (C-compatible, tagged union) */
typedef struct {
    const char* endpoint;
    const char* environment;
    const char* account;
    const char* namespace_name;
    const char* region;
    uint32_t config_major_version;
    int32_t auth_method; /* 0 = Managed Identity, 1 = Certificate */
    const char* tenant;
    const char* role_name;
    const char* role_instance;
    GenevaAuthConfig auth; /* Active member selected by auth_method */
} GenevaConfig;

/* Create a new Geneva client.
   - On success returns GENEVA_SUCCESS and writes *out_handle.
   - On failure returns an error code. */
GenevaError geneva_client_new(const GenevaConfig* config,
                              GenevaClientHandle** out_handle);

/* Frees a Geneva client handle */
void geneva_client_free(GenevaClientHandle* handle);

/* 1) Encode and compress logs into a list of batches (synchronous).
      `data_ptr` is a protobuf-encoded ExportLogsServiceRequest.
      - On success returns GENEVA_SUCCESS and writes *out_list.
      - On failure returns an error code.
      Caller must free *out_list with geneva_batches_free. */
GenevaError geneva_encode_and_compress_logs_v2(GenevaClientHandle* client,
                                               const uint8_t* data_ptr,
                                               size_t data_len,
                                               GenevaBatchList** out_list);

/* 2) Query number of batches in the list. */
size_t geneva_batches_len(const GenevaBatchList* list);

/* 3) Upload a single batch by index (synchronous, no persistence). */
GenevaError geneva_batch_upload_by_index_sync(GenevaClientHandle* client,
                                              const GenevaBatchList* list,
                                              size_t index);

/* 4) Move out a single batch so you don’t keep the whole list.
      Removes index from list (O(1) swap-remove) and returns a single-batch handle. */
GenevaError geneva_batch_take(GenevaBatchList* list,
                              size_t index,
                              GenevaBatch** out_batch);

/* 5) Upload / free the single-batch handle (synchronous upload). */
GenevaError geneva_single_upload_sync(GenevaClientHandle* client,
                                      const GenevaBatch* batch);
void        geneva_single_free(GenevaBatch* batch);

/* 6) Serialize one batch to a stable blob (caller-managed persistence).
      Two-phase copy: if out_buf==NULL or *inout_len<needed, writes required size to *inout_len
      and returns GENEVA_SUCCESS without copying. Caller retries with adequate buffer. */
GenevaError geneva_batch_serialize(const GenevaBatch* batch,
                                   uint8_t* out_buf,
                                   size_t* inout_len);

/* 7) Upload directly from a serialized blob (no need to reconstruct a batch handle). */
GenevaError geneva_upload_serialized_batch_sync(GenevaClientHandle* client,
                                                const uint8_t* blob_ptr,
                                                size_t blob_len);

/* 8) Free the batch list. */
void geneva_batches_free(GenevaBatchList* list);

#ifdef __cplusplus
}
#endif

#endif // GENEVA_FFI_H
