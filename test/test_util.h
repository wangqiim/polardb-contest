#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <string>

// POSIX dependencies
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "const.h"


class TestUser {
    public:
        int64_t id=0;
        char user_id[128]={0};
        char name[128]={0};
        int64_t salary=0;

        bool operator==(const TestUser &rhs) {
            return (id == rhs.id && salary == rhs.salary
                && memcmp(user_id, rhs.user_id, 128) == 0
                && memcmp(name, rhs.name, 128) == 0);
        }
        
        std::string to_string() {
            char buf[500] = {0};
            sprintf(buf, "id: %lld, user_id: %s, name: %s, salary: %lld",
                (long long)id, user_id, name, (long long)salary);
            return std::string(buf);
        }
};

enum TestColumn{Id=0,Userid,Name,Salary};

int drop_datafile() {
    std::string path = disk_dir;
    if (access(path.c_str(), F_OK) == 0) { // if file exist
        return remove(path.c_str());
    }
    return 0;
}

int rmtree(const char path[]) {
    size_t path_len;
    char *full_path;
    DIR *dir;
    struct stat stat_path, stat_entry;
    struct dirent *entry;

    // stat for the path
    stat(path, &stat_path);

    // if path does not exists or is not dir - exit with status -1
    if (S_ISDIR(stat_path.st_mode) == 0) {
        return 0;
    }

    // if not possible to read the directory for this user
    if ((dir = opendir(path)) == NULL) {
        return 0;
    }

    // the length of the path
    path_len = strlen(path);

    // iteration through entries in the directory
    while ((entry = readdir(dir)) != NULL) {

        // skip entries "." and ".."
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
            continue;

        // determinate a full path of an entry
        full_path = (char *)malloc(path_len + strlen(entry->d_name) + 1);
        strcpy(full_path, path);
        strcat(full_path, "/");
        strcat(full_path, entry->d_name);

        // stat for the entry
        stat(full_path, &stat_entry);

        // recursively remove a nested directory
        if (S_ISDIR(stat_entry.st_mode) != 0) {
            rmtree(full_path);
            continue;
        }

        // remove a file object
        if (unlink(full_path) != 0) {
            printf("Can`t remove a file: %s\n", full_path);
            return -1;
        }
        free(full_path);
    }

    // remove the devastated directory and close the object of it
    if (rmdir(path) != 0) {
        printf("Can`t remove a directory: %s\n", path);
        return -1;
    }

    closedir(dir);
    return 0;
}