# CMake generated Testfile for 
# Source directory: /home/thinbt/new thinbt
# Build directory: /home/thinbt/new thinbt/build
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(test_seed "/home/thinbt/new thinbt/build/test_seed")
set_tests_properties(test_seed PROPERTIES  _BACKTRACE_TRIPLES "/home/thinbt/new thinbt/CMakeLists.txt;75;add_test;/home/thinbt/new thinbt/CMakeLists.txt;78;add_thinbt_test;/home/thinbt/new thinbt/CMakeLists.txt;0;")
add_test(test_fastcdc "/home/thinbt/new thinbt/build/test_fastcdc")
set_tests_properties(test_fastcdc PROPERTIES  _BACKTRACE_TRIPLES "/home/thinbt/new thinbt/CMakeLists.txt;75;add_test;/home/thinbt/new thinbt/CMakeLists.txt;81;add_thinbt_test;/home/thinbt/new thinbt/CMakeLists.txt;0;")
add_test(test_chunk_assembler "/home/thinbt/new thinbt/build/test_chunk_assembler")
set_tests_properties(test_chunk_assembler PROPERTIES  _BACKTRACE_TRIPLES "/home/thinbt/new thinbt/CMakeLists.txt;75;add_test;/home/thinbt/new thinbt/CMakeLists.txt;84;add_thinbt_test;/home/thinbt/new thinbt/CMakeLists.txt;0;")
subdirs("third_party")
