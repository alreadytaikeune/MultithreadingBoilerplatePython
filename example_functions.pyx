#!/usr/bin/env cython
cimport cython
from cpython.string cimport PyString_AsString
from libc.stdlib cimport malloc, free
DEF MAX_WORD_LENGTH = 2000


def syracuse(start):
    cdef int start_i = start
    cdef int nb_iterations = 1
    if start < 0:
        return 0
    with nogil:
        while start_i > 1:
            if start_i % 2 == 0:
                start_i = start_i/2
            else:
                start_i = 3*start_i + 1
            nb_iterations += 1
    return nb_iterations


def levenstein(word1, word2):
    cdef int l1 = len(word1)
    cdef int l2 = len(word2)
    cdef int *dynamic = <int *> malloc(l1*l2*sizeof(int))
    cdef int i1 = 0
    cdef int i2 = 0
    cdef char* s1 = PyString_AsString(word1)
    cdef char* s2 = PyString_AsString(word2)
    cdef int l=0
    cdef int c=0
    cdef int m = l2
    with nogil:
        while i2 < l2:
            i1 = 0
            while i1 < l1:
                if i2 == 0:
                    if i1 == 0:
                        dynamic[i1*m + i2] = 0
                    else:
                        dynamic[i1*m+i2] = dynamic[(i1-1)*m + i2] + 1
                elif i1 == 0:
                    dynamic[i1*m+i2] = 0
                else:
                    if s1[i1] != s2[i2]:
                        c = 1
                    else:
                        c = 0
                    dynamic[i1*m+i2] = min(
                        dynamic[(i1-1)*m + i2]+1, dynamic[(i1-1)*m+i2-1]+c, dynamic[i1*m+i2-1]+1)
                i1 += 1
            i2 +=1
    res = dynamic[(l1-1)*m + l2-1]
    free(dynamic)
    return res


