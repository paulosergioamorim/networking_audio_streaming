#ifndef SUFFIX_H
#define SUFFIX_H

int EndsWith(const char *str, const char *suffix);

#ifdef SUFFIX_IMPLEMENTATION
#include <stddef.h>
#include <string.h>

int EndsWith(const char *str, const char *suffix) {
    if (!str || !suffix)
        return 0;
    size_t lenstr = strlen(str);
    size_t lensuffix = strlen(suffix);
    if (lensuffix > lenstr)
        return 0;
    return strncmp(str + lenstr - lensuffix, suffix, lensuffix) == 0;
}

int IsAudioFile(const char *str) {
    return EndsWith(str, ".mp3") || EndsWith(str, ".mp4") || EndsWith(str, ".wav");
}
#endif

#endif
