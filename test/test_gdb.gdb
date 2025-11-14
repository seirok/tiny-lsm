# source ~/tiny-lsm/test/test_gdb.gdb
b test_lsm.cpp:77 if i==2
commands
    b engine.cpp:117 if sst_id==2
    continue
end

run