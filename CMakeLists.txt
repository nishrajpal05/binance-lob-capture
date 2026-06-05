cmake_minimum_required(VERSION 3.16)
project(binance_capture CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(FetchContent)

FetchContent_Declare(
    simdjson
    GIT_REPOSITORY https://github.com/simdjson/simdjson.git
    GIT_TAG        v3.12.3
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(simdjson)

find_package(Boost REQUIRED COMPONENTS system)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

add_executable(binance_capture
    src/main.cpp
    src/fixed_point.cpp
    src/order_book.cpp
    src/csv_writer.cpp
    src/ws_client.cpp
    src/message_handler.cpp
    src/replay.cpp
)

target_include_directories(binance_capture PRIVATE src/)

target_link_libraries(binance_capture PRIVATE
    Boost::system
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    simdjson
)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(binance_capture PRIVATE -Wall -Wextra -O2)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(binance_capture PRIVATE stdc++fs)
endif()