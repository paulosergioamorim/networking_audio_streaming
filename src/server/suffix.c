#include "suffix.h"
#include <stddef.h>
#include <string.h>

int suffix_ends_with(const char *str, const char *suffix) {
    if (!str || !suffix)
        return 0;
    size_t lenstr = strlen(str);
    size_t lensuffix = strlen(suffix);
    if (lensuffix > lenstr)
        return 0;
    return strncmp(str + lenstr - lensuffix, suffix, lensuffix) == 0;
}

int suffix_is_audio(const char *str) {
    // only mp3
    return suffix_ends_with(str, ".mp3");
}
