if [ $1 == '0' ]
then
    zip project0-submission.zip src/include/primer/p0_starter.h
fi

if [ $1 == '1' ]
then
    zip project1-submission.zip \
    src/include/buffer/lru_replacer.h \
    src/buffer/lru_replacer.cpp \
    src/include/buffer/buffer_pool_manager_instance.h \
    src/buffer/buffer_pool_manager_instance.cpp \
    src/include/buffer/parallel_buffer_pool_manager.h \
    src/buffer/parallel_buffer_pool_manager.cpp
fi

if [ $1 == '2' ]
then
    zip project2-submission.zip src/include/buffer/lru_replacer.h \
    src/buffer/lru_replacer.cpp \
    src/include/buffer/buffer_pool_manager_instance.h \
    src/buffer/buffer_pool_manager_instance.cpp \
    src/include/buffer/parallel_buffer_pool_manager.h \
    src/buffer/parallel_buffer_pool_manager.cpp \
    src/include/storage/page/hash_table_directory_page.h \
    src/storage/page/hash_table_directory_page.cpp \
    src/include/storage/page/hash_table_bucket_page.h \
    src/storage/page/hash_table_bucket_page.cpp \
    src/include/container/hash/extendible_hash_table.h \
    src/container/hash/extendible_hash_table.cpp \
    src/include/storage/page/tmp_tuple_page.h
fi

if [ $1 == '3' ]
then
    zip project3-submission.zip \
    src/include/buffer/lru_replacer.h \
    src/buffer/lru_replacer.cpp \
    src/include/buffer/buffer_pool_manager_instance.h \
    src/buffer/buffer_pool_manager_instance.cpp \
    src/include/storage/page/hash_table_directory_page.h \
    src/storage/page/hash_table_directory_page.cpp \
    src/include/storage/page/hash_table_bucket_page.h \
    src/storage/page/hash_table_bucket_page.cpp \
    src/include/container/hash/extendible_hash_table.h \
    src/container/hash/extendible_hash_table.cpp \
    src/include/execution/execution_engine.h \
    src/include/execution/executors/seq_scan_executor.h \
    src/include/execution/executors/insert_executor.h \
    src/include/execution/executors/update_executor.h \
    src/include/execution/executors/delete_executor.h \
    src/include/execution/executors/nested_loop_join_executor.h \
    src/include/execution/executors/hash_join_executor.h \
    src/include/execution/executors/aggregation_executor.h \
    src/include/execution/executors/limit_executor.h \
    src/include/execution/executors/distinct_executor.h \
    src/execution/seq_scan_executor.cpp \
    src/execution/insert_executor.cpp \
    src/execution/update_executor.cpp \
    src/execution/delete_executor.cpp \
    src/execution/nested_loop_join_executor.cpp \
    src/execution/hash_join_executor.cpp \
    src/execution/aggregation_executor.cpp \
    src/execution/limit_executor.cpp \
    src/execution/distinct_executor.cpp \
    src/include/storage/page/tmp_tuple_page.h

fi