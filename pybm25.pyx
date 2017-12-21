# distutils: language = c++
# distutils: sources = bm25.cpp

from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp.string cimport string


cdef extern from "bm25.hpp" namespace "search":
    ctypedef unsigned int uint
    cdef cppclass BM25:
        BM25(string filename)

        vector[pair[uint, double]] search(vector[string] query,
                                    int qty, int sex, int city,
                                    int age_from, int age_to, int relation)

cdef class PyBM25:
    cdef BM25* bm25

    def __cinit__(self, string filename):
        self.bm25 = new BM25(filename)

    def __dealloc__(self):
        del self.bm25

    def search(self, vector[string] query,
                                    int qty, int sex, int city,
                                    int age_from, int age_to, int relation):
        return self.bm25.search(query, qty, sex, city, age_from, age_to, relation)