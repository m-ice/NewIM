#ifndef NEWIM_NATIVE_H
#define NEWIM_NATIVE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define NEWIM_NATIVE_ABI_VERSION 1u
#define NEWIM_NATIVE_MAX_EVENT_BYTES (1024u * 1024u)

typedef uint64_t newim_native_client;

enum newim_native_status {
    NEWIM_NATIVE_OK = 0,
    NEWIM_NATIVE_INVALID_INPUT = 1,
    NEWIM_NATIVE_CLOSED = 2,
    NEWIM_NATIVE_BUSY = 3,
    NEWIM_NATIVE_NO_EVENT = 4,
    NEWIM_NATIVE_BUFFER_TOO_SMALL = 5,
    NEWIM_NATIVE_STORE_UNAVAILABLE = 6,
    NEWIM_NATIVE_INTERNAL_FAILURE = 7
};

uint32_t newim_native_abi_version(void);

/* The caller owns all input memory and the returned handle. */
int32_t newim_native_open(const uint8_t *root, size_t root_len,
                          const uint8_t *account, size_t account_len,
                          newim_native_client *client);

/* Submits one UTF-8 event. Call on the host's serialized SDK worker. */
int32_t newim_native_submit(newim_native_client client,
                            const uint8_t *event, size_t event_len);

/* On BUFFER_TOO_SMALL, writes the required size and preserves the next event. */
int32_t newim_native_next(newim_native_client client,
                          uint8_t *out, size_t capacity, size_t *size);

/* Invalidates the handle and discards any unconsumed events. */
int32_t newim_native_close(newim_native_client client);

#ifdef __cplusplus
}
#endif

#endif
