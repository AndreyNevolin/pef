TARGET = pef
export BUILD_FLAGS =

SRCS = src/pef.cpp src/config.cpp src/worker.cpp
HEADERS = src/common.h src/time.h src/config.h src/worker.h

OUTDIR = .
OBJDIR = .objs
INC = -I../wtmlib/src
EXTRALIBS = -lwtm -L../wtmlib -Wl,-rpath=../wtmlib
FULLTARGET = ${OUTDIR}/${TARGET}

OBJS = $(patsubst src/%.cpp, ${OBJDIR}/%.o, ${SRCS})

GCC = g++ -std=c++0x -Wall -pthread $(BUILD_FLAGS)

.PHONY: clean

default : BUILD_FLAGS += -s
default : ${FULLTARGET}

debug : BUILD_FLAGS += -DPEF_DEBUG -g -O0
debug : ${FULLTARGET}

clean:
	-rm -f ${FULLTARGET} > /dev/null 2>&1
	-rm -f ${OBJS} > /dev/null 2>&1

${FULLTARGET}: ${OBJS}
	-mkdir -p ${OUTDIR} > /dev/null 2>&1
	${GCC} -o ${FULLTARGET} ${OBJS} ${EXTRALIBS}

${OBJDIR}/%.o: src/%.cpp ${HEADERS}
	-mkdir -p ${OBJDIR} > /dev/null 2>&1
	${GCC} ${INC} -c -o $@ $<
