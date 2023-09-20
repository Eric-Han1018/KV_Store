# the compiler: gcc for C program, g++ for C++
CC = g++

# compiler flags:
#  -g    adds debugging information to the executable file
#  -o2 / -o3 to enable optimization
#  -Wall turns on most, but not all, compiler warnings
CFLAGS  = -g -Wall

# the build target executable:
TARGET = db

all: $(TARGET)

$(TARGET): $(TARGET).cpp
	$(CC) $(CFLAGS) -o $(TARGET) $(TARGET).cpp

clean:
	$(RM) $(TARGET)