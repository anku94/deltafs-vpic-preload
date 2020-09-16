#
# test programs
#
set (tests range_utils_test.cc)

foreach (lcv ${tests})

    # use basename as test name
    get_filename_component (id ${lcv} NAME_WE)

    add_executable ("preload_${id}" ${lcv})
    target_link_libraries ("preload_${id}" deltafs-preload)
    add_test("preload_${id}" "preload_${id}")
    add_dependencies (pdl-build-tests "preload_${id}")

endforeach ()
