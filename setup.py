from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension

source_files = ['pybm25.pyx', 'bm25.cpp']
compile_opts = ['-std=c++11']
ext = Extension('*',
                sources=source_files,
                extra_compile_args=compile_opts,
                language='c++')

setup(
    ext_modules=cythonize(ext)
)

# setup(ext_modules=cythonize(
#     "bm25.pyx",                 # our Cython source
#     sources=["bm25.cpp"],  # additional source file(s)
#     language="c++",             # generate C++ code
# ))
