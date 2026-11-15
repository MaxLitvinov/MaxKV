CXX      ?= g++
CXXFLAGS ?= -std=c++20 -O2 -Wall -Wextra -Wpedantic -Iinclude -pthread
SAN      ?=
ifeq ($(SAN),thread)
CXXFLAGS += -fsanitize=thread -g -O1
else ifeq ($(SAN),address)
CXXFLAGS += -fsanitize=address,undefined -g -O1
endif
BUILD := build

LIB_SRC := src/store.cpp src/resp.cpp src/wal.cpp src/engine.cpp src/server.cpp
LIB_OBJ := $(LIB_SRC:src/%.cpp=$(BUILD)/%.o)

.PHONY: all test bench clean
all: $(BUILD)/maxkv $(BUILD)/maxkv_tests $(BUILD)/maxkv_bench

$(BUILD)/%.o: src/%.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -MMD -c $< -o $@
$(BUILD)/maxkv: $(LIB_OBJ) $(BUILD)/main.o
	$(CXX) $(CXXFLAGS) $^ -o $@
$(BUILD)/maxkv_tests: $(LIB_OBJ) tests/test_main.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@
$(BUILD)/maxkv_bench: bench/bench.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@
$(BUILD):
	mkdir -p $(BUILD)

test: $(BUILD)/maxkv_tests
	./$(BUILD)/maxkv_tests
bench: $(BUILD)/maxkv $(BUILD)/maxkv_bench
	./$(BUILD)/maxkv --no-aof --port 6390 & PID=$$!; sleep 0.5; \
	./$(BUILD)/maxkv_bench --port 6390 --clients 32 --requests 200000 --pipeline 16; kill $$PID
clean:
	rm -rf $(BUILD)
-include $(BUILD)/*.d
