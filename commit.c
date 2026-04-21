// commit.c — Commit object implementation
#include "commit.h"
#include "tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

int commit_parse(const void *data, size_t len, Commit *commit_out) {
    memset(commit_out, 0, sizeof(*commit_out));
    const char *ptr = (const char *)data;
    const char *end = ptr + len;

    while (ptr < end) {
        const char *newline = memchr(ptr, '\n', end - ptr);
        if (!newline) break;
        size_t line_len = newline - ptr;
        char line[1024];
        if (line_len >= sizeof(line)) { ptr = newline + 1; continue; }
        memcpy(line, ptr, line_len);
        line[line_len] = '\0';
        ptr = newline + 1;

        if (line_len == 0) {
            size_t msg_len = end - ptr;
            if (msg_len >= sizeof(commit_out->message)) msg_len = sizeof(commit_out->message) - 1;
            memcpy(commit_out->message, ptr, msg_len);
            commit_out->message[msg_len] = '\0';
            break;
        }

        if (strncmp(line, "tree ", 5) == 0)
            hex_to_hash(line + 5, &commit_out->tree);
        else if (strncmp(line, "parent ", 7) == 0) {
            hex_to_hash(line + 7, &commit_out->parent);
            commit_out->has_parent = 1;
        } else if (strncmp(line, "author ", 7) == 0)
            strncpy(commit_out->author, line + 7, sizeof(commit_out->author) - 1);
        else if (strncmp(line, "timestamp ", 10) == 0)
            commit_out->timestamp = (uint64_t)strtoull(line + 10, NULL, 10);
    }
    return 0;
}

int commit_serialize(const Commit *commit, void **data_out, size_t *len_out) {
    char tree_hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&commit->tree, tree_hex);

    char buf[8192];
    int offset = 0;
    offset += snprintf(buf + offset, sizeof(buf) - offset, "tree %s\n", tree_hex);
    if (commit->has_parent) {
        char parent_hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&commit->parent, parent_hex);
        offset += snprintf(buf + offset, sizeof(buf) - offset, "parent %s\n", parent_hex);
    }
    offset += snprintf(buf + offset, sizeof(buf) - offset, "author %s\n", commit->author);
    offset += snprintf(buf + offset, sizeof(buf) - offset, "timestamp %llu\n", (unsigned long long)commit->timestamp);
    offset += snprintf(buf + offset, sizeof(buf) - offset, "\n%s", commit->message);

    *data_out = malloc(offset);
    if (!*data_out) return -1;
    memcpy(*data_out, buf, offset);
    *len_out = offset;
    return 0;
}

int head_read(ObjectID *id_out) {
    char head_path[256];
    snprintf(head_path, sizeof(head_path), "%s/HEAD", PES_DIR);

    FILE *f = fopen(head_path, "r");
    if (!f) return -1;

    char buf[256];
    if (!fgets(buf, sizeof(buf), f)) { fclose(f); return -1; }
    fclose(f);

    // Strip newline
    buf[strcspn(buf, "\n")] = '\0';

    // Check if symbolic ref
    if (strncmp(buf, "ref: ", 5) == 0) {
        char ref_path[512];
        snprintf(ref_path, sizeof(ref_path), "%s/%s", PES_DIR, buf + 5);
        FILE *rf = fopen(ref_path, "r");
        if (!rf) return -1;
        char hex[HASH_HEX_SIZE + 1];
        if (!fgets(hex, sizeof(hex), rf)) { fclose(rf); return -1; }
        fclose(rf);
        hex[strcspn(hex, "\n")] = '\0';
        return hex_to_hash(hex, id_out);
    }

    // Direct hash
    return hex_to_hash(buf, id_out);
}

int head_update(const ObjectID *new_commit) {
    char head_path[256];
    snprintf(head_path, sizeof(head_path), "%s/HEAD", PES_DIR);

    FILE *f = fopen(head_path, "r");
    if (!f) return -1;
    char buf[256];
    if (!fgets(buf, sizeof(buf), f)) { fclose(f); return -1; }
    fclose(f);
    buf[strcspn(buf, "\n")] = '\0';

    char target_path[512];
    if (strncmp(buf, "ref: ", 5) == 0) {
        snprintf(target_path, sizeof(target_path), "%s/%s", PES_DIR, buf + 5);
    } else {
        snprintf(target_path, sizeof(target_path), "%s", head_path);
    }

    // Ensure parent directory exists
    char dir_copy[512];
    strncpy(dir_copy, target_path, sizeof(dir_copy) - 1);
    char *last_slash = strrchr(dir_copy, '/');
    if (last_slash) {
        *last_slash = '\0';
        // mkdir -p style: try creating each component
        for (char *p = dir_copy + 1; *p; p++) {
            if (*p == '/') {
                *p = '\0';
                mkdir(dir_copy, 0755);
                *p = '/';
            }
        }
        mkdir(dir_copy, 0755);
    }

    // Atomic write: temp file + rename
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp_XXXXXX", target_path);
    int fd = mkstemp(tmp_path);
    if (fd < 0) return -1;

    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(new_commit, hex);
    char line[HASH_HEX_SIZE + 2];
    snprintf(line, sizeof(line), "%s\n", hex);
    if (write(fd, line, strlen(line)) != (ssize_t)strlen(line)) {
        close(fd); return -1;
    }
    fsync(fd);
    close(fd);

    if (rename(tmp_path, target_path) != 0) return -1;
    return 0;
}

int commit_walk(commit_walk_fn callback, void *ctx) {
    ObjectID id;
    if (head_read(&id) != 0) return 0; // No commits yet

    while (1) {
        ObjectType type;
        void *data;
        size_t len;
        if (object_read(&id, &type, &data, &len) != 0) return -1;
        if (type != OBJ_COMMIT) { free(data); return -1; }

        Commit commit;
        if (commit_parse(data, len, &commit) != 0) { free(data); return -1; }
        free(data);

        callback(&id, &commit, ctx);

        if (!commit.has_parent) break;
        id = commit.parent;
    }
    return 0;
}

int commit_create(const char *message, ObjectID *commit_id_out) {
    ObjectID tree_id;
    if (tree_from_index(&tree_id) != 0) return -1;

    Commit c;
    memset(&c, 0, sizeof(c));
    c.tree = tree_id;
    c.timestamp = (uint64_t)time(NULL);
    snprintf(c.author, sizeof(c.author), "%s", pes_author());
    snprintf(c.message, sizeof(c.message), "%s", message);

    ObjectID parent_id;
    if (head_read(&parent_id) == 0) {
        c.parent = parent_id;
        c.has_parent = 1;
    } else {
        c.has_parent = 0;
    }

    void *data;
    size_t len;
    if (commit_serialize(&c, &data, &len) != 0) return -1;

    ObjectID commit_id;
    int ret = object_write(OBJ_COMMIT, data, len, &commit_id);
    free(data);
    if (ret != 0) return -1;

    if (head_update(&commit_id) != 0) return -1;
    if (commit_id_out) *commit_id_out = commit_id;
    return 0;
}
