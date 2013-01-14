# whatever flags you want to pass to the C compiler & linker
AM_CFLAGS = --pedantic -Wall -std=c99 -O2
AM_LDFLAGS = -lm

lib_LIBRARIES = libordacity.a
libordacity_a_SOURCES = ClusterConfig.c Cluster.c queue_lock_mutex.c
 
include_HEADERS = ClusterConfig.h ClusterListener.h
