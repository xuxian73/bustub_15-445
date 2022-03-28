rm handin.zip
if [ $1 == '0' ]; then
    zip handin.zip src/include/primer/p0_starter.h
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
