# source ~/tiny-lsm/test/test_gdb.gdb
b test_lsm.cpp:79 if i==2
commands
    b engine.cpp:117 if sst_id==2
    commands
       # b sst_iterator.cpp:138
       # commands

       # end
       # continue
    end
    continue
end

run