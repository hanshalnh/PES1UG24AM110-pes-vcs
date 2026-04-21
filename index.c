// index.c — Staging area implementation
#include "index.h"
#include <stdio.h>
#include "pes.h"
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");
    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else if (st.st_mtime != (time_t)index->entries[i].mtime_sec ||
                   st.st_size != (off_t)index->entries[i].size) {
            printf("  modified:   %s\n", index->entries[i].path);
            unstaged_count++;
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");
    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue;
            if (strstr(ent->d_name, ".o") != NULL) continue;
            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) { is_tracked = 1; break; }
            }
            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) {
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");
    return 0;
}

static int compare_entries(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path, ((const IndexEntry *)b)->path);
}

int index_load(Index *index) {
    index->count = 0;
    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) return 0;
    char hex[HASH_HEX_SIZE + 1];
    while (index->count < MAX_INDEX_ENTRIES) {
        IndexEntry *e = &index->entries[index->count];
        int ret = fscanf(f, "%o %64s %llu %u %511s",
                         &e->mode, hex,
                         (unsigned long long *)&e->mtime_sec,
                         &e->size, e->path);
        if (ret != 5) break;
        if (hex_to_hash(hex, &e->hash) != 0) { fclose(f); return -1; }
        index->count++;
    }
    fclose(f);
    return 0;
}

int index_save(const Index *index) {
    Index *sorted = malloc(sizeof(Index));
    if (!sorted) return -1;
    *sorted = *index;
    qsort(sorted->entries, sorted->count, sizeof(IndexEntry), compare_entries);
    char tmp_path[256];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp_XXXXXX", INDEX_FILE);
    int fd = mkstemp(tmp_path);
    if (fd < 0) return -1;
    FILE *f = fdopen(fd, "w");
    if (!f) { close(fd); return -1; }
    for (int i = 0; i < sorted->count; i++) {
        char hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&sorted->entries[i].hash, hex);
        fprintf(f, "%o %s %llu %u %s\n",
                sorted->entries[i].mode, hex,
                (unsigned long long)sorted->entries[i].mtime_sec,
                sorted->entries[i].size,
                sorted->entries[i].path);
    }
    fflush(f);
    fsync(fileno(f));
    fclose(f);
    if (rename(tmp_path, INDEX_FILE) != 0) { free(sorted); return -1; }
    free(sorted);
    return 0;
}

int index_add(Index *index, const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) { fprintf(stderr, "error: cannot open '%s'\n", path); return -1; }
    fseek(f, 0, SEEK_END);
    size_t size = ftell(f);
    fseek(f, 0, SEEK_SET);
    void *data = malloc(size ? size : 1);
    if (!data) { fclose(f); return -1; }
    if (size) { int r = fread(data, 1, size, f); (void)r; }
    fclose(f);
    ObjectID hash;
    if (object_write(OBJ_BLOB, data, size, &hash) != 0) { free(data); return -1; }
    free(data);
    struct stat st;
    if (lstat(path, &st) != 0) return -1;
    IndexEntry *existing = index_find(index, path);
    if (!existing) {
        if (index->count >= MAX_INDEX_ENTRIES) return -1;
        existing = &index->entries[index->count++];
    }
    existing->mode = (st.st_mode & S_IXUSR) ? 0100755 : 0100644;
    existing->hash = hash;
    existing->mtime_sec = st.st_mtime;
    existing->size = st.st_size;
    strncpy(existing->path, path, sizeof(existing->path) - 1);
    existing->path[sizeof(existing->path) - 1] = '\0';
    return index_save(index);
}
