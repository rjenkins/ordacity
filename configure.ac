#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.61)
AC_INIT(ordacity, 0.1, ray@boundary.com)
AM_INIT_AUTOMAKE(ordacity, 0.1) 
AM_INIT_AUTOMAKE([subdir-objects])

AC_PROG_RANLIB

# Checks for programs.
AC_PROG_CC

#Zookeeper

CPPFLAGS="-I/Users/rjenkins/Downloads/zookeeper-3.4.3/src/c/include -I/Users/rjenkins/Downloads/zookeeper-3.4.3/src/c/src/hashtable -I/Users/rjenkins/Downloads/zookeeper-3.4.3/src/c/generated";

AC_ARG_ENABLE(debug, "used to turn on the DEBUG flag",
  [ AC_DEFINE(DEBUG) ])

# Checks for libraries.
# FIXME: Replace `main' with a function in `-lm':
AC_CHECK_LIB([m], [main])

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

AC_CONFIG_FILES([Makefile
                 src/Makefile])
AC_OUTPUT
