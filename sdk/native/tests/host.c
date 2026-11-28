#define _POSIX_C_SOURCE 200809L

#include "newim_native.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int open_client(const char *root, const char *account, newim_native_client *client) {
    return newim_native_open((const uint8_t *)root, strlen(root),
                             (const uint8_t *)account, strlen(account), client);
}

static int submit(newim_native_client client, const char *event) {
    return newim_native_submit(client, (const uint8_t *)event, strlen(event));
}

static void check(int condition, const char *label) {
    if (!condition) {
        fprintf(stderr, "native ABI check failed: %s\n", label);
        exit(1);
    }
}

static void smoke(const char *root, const char *account) {
    const char *network = "{\"nowMs\":10,\"event\":{\"type\":\"network_available\"}}";
    newim_native_client first = 0, second = 9;
    uint8_t buffer[512];
    size_t size = 11, retry = 0;
    check(newim_native_abi_version() == NEWIM_NATIVE_ABI_VERSION, "version");
    check(open_client(root, account, &first) == NEWIM_NATIVE_OK && first != 0, "open");
    check(open_client(root, account, &second) == NEWIM_NATIVE_BUSY && second == 0, "exclusive store");
    check(newim_native_next(first, NULL, 0, &size) == NEWIM_NATIVE_NO_EVENT && size == 0, "empty");
    check(newim_native_submit(first, NULL, 1) == NEWIM_NATIVE_INVALID_INPUT, "missing event buffer");
    check(newim_native_submit(first, (const uint8_t *)network,
                              NEWIM_NATIVE_MAX_EVENT_BYTES + 1u) == NEWIM_NATIVE_INVALID_INPUT,
          "oversized input rejected before read");
    check(submit(first, "{\"nowMs\":1,\"nowMs\":2,\"event\":{\"type\":\"tick\"}}")
          == NEWIM_NATIVE_INVALID_INPUT, "duplicate fields");
    check(submit(first, network) == NEWIM_NATIVE_OK, "start");
    check(submit(first, network) == NEWIM_NATIVE_BUSY, "queue backpressure");
    check(newim_native_next(first, NULL, 0, &size) == NEWIM_NATIVE_BUFFER_TOO_SMALL &&
          size > 0 && size < sizeof buffer, "size probe");
    check(newim_native_next(first, buffer, size-1, &retry) == NEWIM_NATIVE_BUFFER_TOO_SMALL &&
          retry == size, "preserve after insufficient capacity");
    check(newim_native_next(first, buffer, sizeof buffer, &retry) == NEWIM_NATIVE_OK && retry == size,
          "read event once");
    buffer[size] = 0;
    check(strstr((const char *)buffer, "\"type\":\"credentials_required\"") != NULL, "credentials effect");
    check(newim_native_next(first, NULL, 0, &size) == NEWIM_NATIVE_NO_EVENT && size == 0, "drained");
    check(newim_native_close(first) == NEWIM_NATIVE_OK, "close");
    check(newim_native_close(first) == NEWIM_NATIVE_CLOSED, "double close");
    check(submit(first, network) == NEWIM_NATIVE_CLOSED, "closed callback");
    check(open_client(root, "another_account", &second) == NEWIM_NATIVE_STORE_UNAVAILABLE &&
          second == 0, "account isolation");
    char other[129];
    memset(other, 'a', sizeof other - 1);
    other[sizeof other - 1] = 0;
    check(open_client(root, other, &second) == NEWIM_NATIVE_STORE_UNAVAILABLE && second == 0,
          "full-width account identifier");
    check(open_client(root, account, &second) == NEWIM_NATIVE_OK && second != first, "reopen with fresh handle");
    check(newim_native_next(second, NULL, 0, &size) == NEWIM_NATIVE_NO_EVENT && size == 0, "no inherited output");
    check(newim_native_close(second) == NEWIM_NATIVE_OK, "close reopened store");
    puts("native C ABI checks passed");
}

static void relay(const char *root, const char *account) {
    newim_native_client client = 0;
    uint8_t *output = malloc(NEWIM_NATIVE_MAX_EVENT_BYTES);
    char *line = NULL;
    size_t cap = 0;
    ssize_t count;
    check(output != NULL, "event buffer");
    check(open_client(root, account, &client) == NEWIM_NATIVE_OK, "open relay client");
    puts("READY");
    fflush(stdout);
    while ((count = getline(&line, &cap, stdin)) > 0) {
        size_t size = 0;
        if (line[count-1] == '\n') {
            line[count-1] = '\0';
        }
        if (strncmp(line, "SUBMIT\t", 7) == 0) {
            printf("SUBMIT\t%d\n", submit(client, line+7));
        } else if (strcmp(line, "NEXT") == 0) {
            int status = newim_native_next(client, output, NEWIM_NATIVE_MAX_EVENT_BYTES, &size);
            printf("NEXT\t%d\t", status);
            if (status == NEWIM_NATIVE_OK) {
                check(fwrite(output, 1, size, stdout) == size, "write event");
            }
            putchar('\n');
        } else if (strcmp(line, "REOPEN") == 0) {
            check(newim_native_close(client) == NEWIM_NATIVE_OK, "close before reopen");
            check(open_client(root, account, &client) == NEWIM_NATIVE_OK, "reopen relay client");
            puts("READY");
        } else if (strcmp(line, "QUIT") == 0) {
            puts("DONE");
            break;
        } else {
            check(0, "relay command");
        }
        fflush(stdout);
    }
    free(line);
    free(output);
    check(newim_native_close(client) == NEWIM_NATIVE_OK, "close relay client");
}

int main(int argc, char **argv) {
    check(argc == 4, "usage");
    if (strcmp(argv[1], "--check") == 0) {
        smoke(argv[2], argv[3]);
    } else {
        check(strcmp(argv[1], "--relay") == 0, "command");
        relay(argv[2], argv[3]);
    }
    return 0;
}
