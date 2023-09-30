# ------------------------------------------------------------------------------------------------------------------------#
# Makefile, modified from https://stackoverflow.com/questions/66291366/c-makefile-path-include-and-src-directory-makefile #
# ------------------------------------------------------------------------------------------------------------------------#
SRC_DIR := src
OBJ_DIR := obj
BIN_DIR := bin
DATA_DIR := data

EXE := $(BIN_DIR)/db
SRC := $(wildcard $(SRC_DIR)/*.cpp)
OBJ := $(SRC:$(SRC_DIR)/%.cpp=$(OBJ_DIR)/%.o)

CC := g++
# C PreProcessor Flags
# -I	adds include directory of header files
# -MMD  outputs a Makefile snippet for each compiled .cpp and save to a .d file 
# -MP   outputs a non-dependent target for each header file
CPPFLAGS := -Iinclude -MMD -MP -std=c++11
# Compiler Flags:
#  -g    adds debugging information to the executable file
#  -o2 / -o3 to enable optimization
#  -Wall turns on most, but not all, compiler warnings
CFLAGS   := -g -Wall
# Pass extra flags to the linker
# -L	 specify directories where the libraries can be found
LDFLAGS  := -Llib
# Third-party libraries that are used
LDLIBS   :=


.PHONY: all clean

all: $(EXE)

$(EXE): $(OBJ) | $(BIN_DIR)
	$(CC) $(LDFLAGS) $^ $(LDLIBS) -o $@

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp | $(OBJ_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(BIN_DIR) $(OBJ_DIR):
	mkdir -p $@

clean:
	@$(RM) -rv $(EXE) $(OBJ_DIR) $(DATA_DIR)/*

-include $(OBJ:.o=.d)