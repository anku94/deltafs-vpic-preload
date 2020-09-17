#
# test programs
#
set (tests range_utils_test.cc)

foreach (lcv ${tests})

    # use basename as test name
    get_filename_component (id ${lcv} NAME_WE)

    add_executable (${id} ${lcv})
    target_link_libraries (${id} deltafs-preload)
    add_test(${id} ${id})
    add_dependencies (pdl-build-tests ${id})

endforeach ()
