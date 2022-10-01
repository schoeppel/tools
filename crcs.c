#define _GNU_SOURCE

#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <dirent.h>

#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>

#include <time.h>


static uint64_t nanotime() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    
    return ts.tv_sec * 1000000000UL + ts.tv_nsec;
}

static uint32_t crc32c_hw(const char *buf, uint64_t len) {
    uint64_t crc = 0xffffffff;
    assert((uintptr_t)buf % 8 == 0);

    while (len >= 32) {
        __asm__("crc32q\t (%1), %0\n"
                "crc32q\t 8(%1), %0\n"
                "crc32q\t 16(%1), %0\n"
                "crc32q\t 24(%1), %0"
                : "+r"(crc) : "r"(buf));
        buf+=32;
        len-=32;
    }

    while (len) {
        __asm__("crc32b\t" "(%1), %0"
                : "+r"(crc)
                : "r"(buf), "m"(*buf));
        buf++;
        len--;
    }

    return ~(uint32_t)crc;
}

static int calc_file_crc(const char* filename, uint32_t *crc_out) {
    int fd = -1, ret = -1;
    char *mem = MAP_FAILED;
    uint64_t size = 0;

    fd = open(filename, O_RDONLY);
    if (fd == -1) {
        perror(filename);
        goto out;
    }

    struct stat sb;
    if (fstat(fd, &sb)) {
        perror(filename);
        goto out;
    }
    size = sb.st_size;

    mem = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mem == MAP_FAILED) {
        perror(filename);
        goto out;
    }

    *crc_out = crc32c_hw(mem, size);
    ret = 0;

out:
    if (mem != MAP_FAILED) munmap(mem, size);
    if (fd != -1) close(fd);
    return ret;
}

struct crc_entry {
    ino_t ino;
    uint64_t mtime;
    uint32_t crc;
};

static struct crc_entry* load_crcfile() {
    FILE *f = fopen("CRCS", "r");
    if (! f) return NULL;

    int n = 0;
    char lb[320];
    while (fgets(lb, sizeof(lb), f)) {
        n++;
    }
    rewind(f);
    struct crc_entry *crcs = malloc(sizeof(struct crc_entry)*(n+1));

    int i = 0;
    struct stat sb;
    while (fgets(lb, sizeof(lb), f)) {
        char filename[256];
        if (sscanf(lb, "%x %lu\t%255[^\n]", &(crcs[i].crc), &(crcs[i].mtime), filename) == 3) {
            if (stat(filename, &sb)) continue;
            crcs[i].ino = sb.st_ino;
            i++;
            if (i == n) break;
        }
    }

    crcs[i].ino = 0;
    
    fclose(f);

    return crcs;
}

static int check_crcfile() {
    FILE *f = fopen("CRCS", "r");
    if (! f) return 2;
    
    int n_nfound = 0;
    int n_changed = 0;
    int n_broken = 0;
    int n_err = 0;
    
    char lb[320];
    struct stat sb;
    while (fgets(lb, sizeof(lb), f)) {
        uint32_t crc;
        uint64_t mtime;
        char filename[256];
        if (sscanf(lb, "%x %lu\t%255[^\n]", &crc, &mtime, filename) == 3) {
            if (stat(filename, &sb)) {
                printf("NFOUND %s\n", filename);
                n_nfound++;
                continue;
            }
            
            if (sb.st_mtime != mtime) {
                printf("MODIFIED %s\n", filename);
                n_changed++;
                continue;
            }
            
            uint32_t crc_calc;
            uint64_t start_time = nanotime();
            if (! calc_file_crc(filename, &crc_calc)) {
                uint64_t duration = (nanotime() - start_time) / 1000;
                if (duration) {
                    printf("%6.1f MB/s ", ((double)sb.st_size) / duration);
                }
                
                if (crc == crc_calc) {
                    printf("    OK %s \n", filename);
                } else {
                    printf("BROKEN %s\n", filename);
                    n_broken++;
                }
                
                
            } else {
                n_err++;
            }
         
        }
    }
    
    fclose(f);
    
    if (n_nfound == 0 && n_changed == 0 && n_broken == 0 && n_err == 0) {
        printf("All Files OK\n");
        return 0;
    } else {
        printf("%u Files not found\n%u Files changed\n%u Files broken\n%u Other Errors\n", n_nfound, n_changed, n_broken, n_err);
        return 1;
    }
}

static int update_crcfile() {
    struct crc_entry* old_crcs = load_crcfile();
    
    FILE *f = fopen("CRCS", "w");
    if (! f) return 1;

    DIR *dir;
    struct dirent *entry;

    if ((dir = opendir(".")) == NULL) return 1;

    while ((entry = readdir(dir)) != NULL) {
        if (!strcmp(entry->d_name, "CRCS")) continue;
        
        struct stat sb;
        if (stat(entry->d_name, &sb)) {
            perror(entry->d_name);
            continue;
        }
        if (! S_ISREG(sb.st_mode)) continue;
        
        int found = 0;
        
        struct crc_entry* old = old_crcs;
        while (old && old->ino) {
            if (old->ino == sb.st_ino && old->mtime == sb.st_mtime) {
                found = 1;
                break;
            }
            
            old++;
        }
        
        if (found) {
            printf("      FOUND %08x %010lu\t%s\n", old->crc, (uint64_t)old->mtime, entry->d_name);
            fprintf(f, "%08x %010lu\t%s\n", old->crc, (uint64_t)old->mtime, entry->d_name);
        } else {
            uint32_t crc;
            uint64_t start_time = nanotime();
            
            if (! calc_file_crc(entry->d_name, &crc)) {
                uint64_t duration = (nanotime() - start_time) / 1000;
                if (duration) {
                    printf("%6.1f MB/s ", ((double)sb.st_size) / duration);
                }
                
                printf("%08x %010lu\t%s\n", crc, (uint64_t)sb.st_mtime, entry->d_name);
                fprintf(f, "%08x %010lu\t%s\n", crc, (uint64_t)sb.st_mtime, entry->d_name);
            }
        }
    }

    closedir(dir);
    free(old_crcs);
    fclose(f);
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 2 || (!strcmp(argv[1], "calc") && argc < 3)) {
        fprintf(stderr, "Usage: %s calc <filename> | update | check\n", argv[0]);
        return 1;
    }

    if (!strcmp(argv[1], "calc")) {
        uint32_t crc;
        if (calc_file_crc(argv[2], &crc)) {
            return 1;
        }
        printf("%08x", crc);
        return 0;
    }

    if (!strcmp(argv[1], "update")) {
        update_crcfile();
        return 0;
    }
    
    if (!strcmp(argv[1], "check")) {
        return check_crcfile();
    }

    return 1;
}
