# Makefile.in generated by automake 1.10 from Makefile.am.
# Makefile.  Generated from Makefile.in by configure.

# Copyright (C) 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002,
# 2003, 2004, 2005, 2006  Free Software Foundation, Inc.
# This Makefile.in is free software; the Free Software Foundation
# gives unlimited permission to copy and/or distribute it,
# with or without modifications, as long as this notice is preserved.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY, to the extent permitted by law; without
# even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE.





pkgdatadir = $(datadir)/ordacity
pkglibdir = $(libdir)/ordacity
pkgincludedir = $(includedir)/ordacity
am__cd = CDPATH="$${ZSH_VERSION+.}$(PATH_SEPARATOR)" && cd
install_sh_DATA = $(install_sh) -c -m 644
install_sh_PROGRAM = $(install_sh) -c
install_sh_SCRIPT = $(install_sh) -c
INSTALL_HEADER = $(INSTALL_DATA)
transform = $(program_transform_name)
NORMAL_INSTALL = :
PRE_INSTALL = :
POST_INSTALL = :
NORMAL_UNINSTALL = :
PRE_UNINSTALL = :
POST_UNINSTALL = :
subdir = .
DIST_COMMON = $(am__configure_deps) $(include_HEADERS) \
	$(srcdir)/Makefile.am $(srcdir)/Makefile.in \
	$(top_srcdir)/configure $(top_srcdir)/src/Makefile.in depcomp \
	install-sh missing
ACLOCAL_M4 = $(top_srcdir)/aclocal.m4
am__aclocal_m4_deps = $(top_srcdir)/configure.ac
am__configure_deps = $(am__aclocal_m4_deps) $(CONFIGURE_DEPENDENCIES) \
	$(ACLOCAL_M4)
am__CONFIG_DISTCLEAN_FILES = config.status config.cache config.log \
 configure.lineno config.status.lineno
mkinstalldirs = $(install_sh) -d
CONFIG_CLEAN_FILES = src/Makefile
am__vpath_adj_setup = srcdirstrip=`echo "$(srcdir)" | sed 's|.|.|g'`;
am__vpath_adj = case $$p in \
    $(srcdir)/*) f=`echo "$$p" | sed "s|^$$srcdirstrip/||"`;; \
    *) f=$$p;; \
  esac;
am__strip_dir = `echo $$p | sed -e 's|^.*/||'`;
am__installdirs = "$(DESTDIR)$(libdir)" "$(DESTDIR)$(includedir)"
libLIBRARIES_INSTALL = $(INSTALL_DATA)
LIBRARIES = $(lib_LIBRARIES)
AR = ar
ARFLAGS = cru
libordacity_a_AR = $(AR) $(ARFLAGS)
libordacity_a_LIBADD =
am__dirstamp = $(am__leading_dot)dirstamp
am_libordacity_a_OBJECTS = src/cluster/Cluster.$(OBJEXT) \
	src/cluster/NodeInfo.$(OBJEXT) \
	src/cluster/ClusterUtil.$(OBJEXT) \
	src/collection/queue_lock_mutex.$(OBJEXT) \
	src/collection/hashset.$(OBJEXT) src/jsmn/jsmn.$(OBJEXT)
libordacity_a_OBJECTS = $(am_libordacity_a_OBJECTS)
DEFAULT_INCLUDES = -I.
depcomp = $(SHELL) $(top_srcdir)/depcomp
am__depfiles_maybe = depfiles
COMPILE = $(CC) $(DEFS) $(DEFAULT_INCLUDES) $(INCLUDES) $(AM_CPPFLAGS) \
	$(CPPFLAGS) $(AM_CFLAGS) $(CFLAGS)
CCLD = $(CC)
LINK = $(CCLD) $(AM_CFLAGS) $(CFLAGS) $(AM_LDFLAGS) $(LDFLAGS) -o $@
SOURCES = $(libordacity_a_SOURCES)
DIST_SOURCES = $(libordacity_a_SOURCES)
includeHEADERS_INSTALL = $(INSTALL_HEADER)
HEADERS = $(include_HEADERS)
ETAGS = etags
CTAGS = ctags
DISTFILES = $(DIST_COMMON) $(DIST_SOURCES) $(TEXINFOS) $(EXTRA_DIST)
distdir = $(PACKAGE)-$(VERSION)
top_distdir = $(distdir)
am__remove_distdir = \
  { test ! -d $(distdir) \
    || { find $(distdir) -type d ! -perm -200 -exec chmod u+w {} ';' \
         && rm -fr $(distdir); }; }
DIST_ARCHIVES = $(distdir).tar.gz
GZIP_ENV = --best
distuninstallcheck_listfiles = find . -type f -print
distcleancheck_listfiles = find . -type f -print
ACLOCAL = ${SHELL} /Users/rjenkins/Documents/Boundary/ordacity/missing --run aclocal-1.10
AMTAR = ${SHELL} /Users/rjenkins/Documents/Boundary/ordacity/missing --run tar
AUTOCONF = ${SHELL} /Users/rjenkins/Documents/Boundary/ordacity/missing --run autoconf
AUTOHEADER = ${SHELL} /Users/rjenkins/Documents/Boundary/ordacity/missing --run autoheader
AUTOMAKE = ${SHELL} /Users/rjenkins/Documents/Boundary/ordacity/missing --run automake-1.10
AWK = awk
CC = gcc
CCDEPMODE = depmode=gcc3
CFLAGS = -g -O2
CPPFLAGS = -I/Users/rjenkins/Downloads/zookeeper-3.4.3/src/c/include -I/Users/rjenkins/Downloads/zookeeper-3.4.3/src/c/src/hashtable -I/Users/rjenkins/Downloads/zookeeper-3.4.3/src/c/generated
CYGPATH_W = echo
DEFS = -DPACKAGE_NAME=\"ordacity\" -DPACKAGE_TARNAME=\"ordacity\" -DPACKAGE_VERSION=\"0.1\" -DPACKAGE_STRING=\"ordacity\ 0.1\" -DPACKAGE_BUGREPORT=\"ray@boundary.com\" -DPACKAGE=\"ordacity\" -DVERSION=\"0.1\" -DPACKAGE=\"ordacity\" -DVERSION=\"0.1\" -DDEBUG=1 -DHAVE_LIBM=1
DEPDIR = .deps
ECHO_C = \c
ECHO_N = 
ECHO_T = 
EXEEXT = 
INSTALL = /usr/bin/install -c
INSTALL_DATA = ${INSTALL} -m 644
INSTALL_PROGRAM = ${INSTALL}
INSTALL_SCRIPT = ${INSTALL}
INSTALL_STRIP_PROGRAM = $(install_sh) -c -s
LDFLAGS = 
LIBOBJS = 
LIBS = -lm 
LTLIBOBJS = 
MAKEINFO = ${SHELL} /Users/rjenkins/Documents/Boundary/ordacity/missing --run makeinfo
MKDIR_P = ./install-sh -c -d
OBJEXT = o
PACKAGE = ordacity
PACKAGE_BUGREPORT = ray@boundary.com
PACKAGE_NAME = ordacity
PACKAGE_STRING = ordacity 0.1
PACKAGE_TARNAME = ordacity
PACKAGE_VERSION = 0.1
PATH_SEPARATOR = :
RANLIB = ranlib
SET_MAKE = 
SHELL = /bin/sh
STRIP = 
VERSION = 0.1
abs_builddir = /Users/rjenkins/Documents/Boundary/ordacity
abs_srcdir = /Users/rjenkins/Documents/Boundary/ordacity
abs_top_builddir = /Users/rjenkins/Documents/Boundary/ordacity
abs_top_srcdir = /Users/rjenkins/Documents/Boundary/ordacity
ac_ct_CC = gcc
am__include = include
am__leading_dot = .
am__quote = 
am__tar = ${AMTAR} chof - "$$tardir"
am__untar = ${AMTAR} xf -
bindir = ${exec_prefix}/bin
build_alias = 
builddir = .
datadir = ${datarootdir}
datarootdir = ${prefix}/share
docdir = ${datarootdir}/doc/${PACKAGE_TARNAME}
dvidir = ${docdir}
exec_prefix = ${prefix}
host_alias = 
htmldir = ${docdir}
includedir = ${prefix}/include
infodir = ${datarootdir}/info
install_sh = $(SHELL) /Users/rjenkins/Documents/Boundary/ordacity/install-sh
libdir = ${exec_prefix}/lib
libexecdir = ${exec_prefix}/libexec
localedir = ${datarootdir}/locale
localstatedir = ${prefix}/var
mandir = ${datarootdir}/man
mkdir_p = $(top_builddir)/./install-sh -c -d
oldincludedir = /usr/include
pdfdir = ${docdir}
prefix = /usr/local
program_transform_name = s,x,x,
psdir = ${docdir}
sbindir = ${exec_prefix}/sbin
sharedstatedir = ${prefix}/com
srcdir = .
sysconfdir = ${prefix}/etc
target_alias = 
top_builddir = .
top_srcdir = .
AUTOMAKE_OPTIONS = foreign

# whatever flags you want to pass to the C compiler & linker
AM_CFLAGS = --pedantic -Wall -std=c99 -O2 
AM_LDFLAGS = -lm 
lib_LIBRARIES = libordacity.a
libordacity_a_SOURCES = src/cluster/Cluster.c src/cluster/NodeInfo.c src/cluster/ClusterUtil.c src/collection/queue_lock_mutex.c src/collection/hashset.c src/jsmn/jsmn.c
include_HEADERS = src/cluster/ClusterConfig.h src/cluster/ClusterListener.h src/jsmn/jsmn.h src/cluster/ClusterUtil.h
all: all-am

.SUFFIXES:
.SUFFIXES: .c .o .obj
am--refresh:
	@:
$(srcdir)/Makefile.in:  $(srcdir)/Makefile.am  $(am__configure_deps)
	@for dep in $?; do \
	  case '$(am__configure_deps)' in \
	    *$$dep*) \
	      echo ' cd $(srcdir) && $(AUTOMAKE) --foreign '; \
	      cd $(srcdir) && $(AUTOMAKE) --foreign  \
		&& exit 0; \
	      exit 1;; \
	  esac; \
	done; \
	echo ' cd $(top_srcdir) && $(AUTOMAKE) --foreign  Makefile'; \
	cd $(top_srcdir) && \
	  $(AUTOMAKE) --foreign  Makefile
.PRECIOUS: Makefile
Makefile: $(srcdir)/Makefile.in $(top_builddir)/config.status
	@case '$?' in \
	  *config.status*) \
	    echo ' $(SHELL) ./config.status'; \
	    $(SHELL) ./config.status;; \
	  *) \
	    echo ' cd $(top_builddir) && $(SHELL) ./config.status $@ $(am__depfiles_maybe)'; \
	    cd $(top_builddir) && $(SHELL) ./config.status $@ $(am__depfiles_maybe);; \
	esac;

$(top_builddir)/config.status: $(top_srcdir)/configure $(CONFIG_STATUS_DEPENDENCIES)
	$(SHELL) ./config.status --recheck

$(top_srcdir)/configure:  $(am__configure_deps)
	cd $(srcdir) && $(AUTOCONF)
$(ACLOCAL_M4):  $(am__aclocal_m4_deps)
	cd $(srcdir) && $(ACLOCAL) $(ACLOCAL_AMFLAGS)
src/Makefile: $(top_builddir)/config.status $(top_srcdir)/src/Makefile.in
	cd $(top_builddir) && $(SHELL) ./config.status $@
install-libLIBRARIES: $(lib_LIBRARIES)
	@$(NORMAL_INSTALL)
	test -z "$(libdir)" || $(MKDIR_P) "$(DESTDIR)$(libdir)"
	@list='$(lib_LIBRARIES)'; for p in $$list; do \
	  if test -f $$p; then \
	    f=$(am__strip_dir) \
	    echo " $(libLIBRARIES_INSTALL) '$$p' '$(DESTDIR)$(libdir)/$$f'"; \
	    $(libLIBRARIES_INSTALL) "$$p" "$(DESTDIR)$(libdir)/$$f"; \
	  else :; fi; \
	done
	@$(POST_INSTALL)
	@list='$(lib_LIBRARIES)'; for p in $$list; do \
	  if test -f $$p; then \
	    p=$(am__strip_dir) \
	    echo " $(RANLIB) '$(DESTDIR)$(libdir)/$$p'"; \
	    $(RANLIB) "$(DESTDIR)$(libdir)/$$p"; \
	  else :; fi; \
	done

uninstall-libLIBRARIES:
	@$(NORMAL_UNINSTALL)
	@list='$(lib_LIBRARIES)'; for p in $$list; do \
	  p=$(am__strip_dir) \
	  echo " rm -f '$(DESTDIR)$(libdir)/$$p'"; \
	  rm -f "$(DESTDIR)$(libdir)/$$p"; \
	done

clean-libLIBRARIES:
	-test -z "$(lib_LIBRARIES)" || rm -f $(lib_LIBRARIES)
src/cluster/$(am__dirstamp):
	@$(MKDIR_P) src/cluster
	@: > src/cluster/$(am__dirstamp)
src/cluster/$(DEPDIR)/$(am__dirstamp):
	@$(MKDIR_P) src/cluster/$(DEPDIR)
	@: > src/cluster/$(DEPDIR)/$(am__dirstamp)
src/cluster/Cluster.$(OBJEXT): src/cluster/$(am__dirstamp) \
	src/cluster/$(DEPDIR)/$(am__dirstamp)
src/cluster/NodeInfo.$(OBJEXT): src/cluster/$(am__dirstamp) \
	src/cluster/$(DEPDIR)/$(am__dirstamp)
src/cluster/ClusterUtil.$(OBJEXT): src/cluster/$(am__dirstamp) \
	src/cluster/$(DEPDIR)/$(am__dirstamp)
src/collection/$(am__dirstamp):
	@$(MKDIR_P) src/collection
	@: > src/collection/$(am__dirstamp)
src/collection/$(DEPDIR)/$(am__dirstamp):
	@$(MKDIR_P) src/collection/$(DEPDIR)
	@: > src/collection/$(DEPDIR)/$(am__dirstamp)
src/collection/queue_lock_mutex.$(OBJEXT):  \
	src/collection/$(am__dirstamp) \
	src/collection/$(DEPDIR)/$(am__dirstamp)
src/collection/hashset.$(OBJEXT): src/collection/$(am__dirstamp) \
	src/collection/$(DEPDIR)/$(am__dirstamp)
src/jsmn/$(am__dirstamp):
	@$(MKDIR_P) src/jsmn
	@: > src/jsmn/$(am__dirstamp)
src/jsmn/$(DEPDIR)/$(am__dirstamp):
	@$(MKDIR_P) src/jsmn/$(DEPDIR)
	@: > src/jsmn/$(DEPDIR)/$(am__dirstamp)
src/jsmn/jsmn.$(OBJEXT): src/jsmn/$(am__dirstamp) \
	src/jsmn/$(DEPDIR)/$(am__dirstamp)
libordacity.a: $(libordacity_a_OBJECTS) $(libordacity_a_DEPENDENCIES) 
	-rm -f libordacity.a
	$(libordacity_a_AR) libordacity.a $(libordacity_a_OBJECTS) $(libordacity_a_LIBADD)
	$(RANLIB) libordacity.a

mostlyclean-compile:
	-rm -f *.$(OBJEXT)
	-rm -f src/cluster/Cluster.$(OBJEXT)
	-rm -f src/cluster/ClusterUtil.$(OBJEXT)
	-rm -f src/cluster/NodeInfo.$(OBJEXT)
	-rm -f src/collection/hashset.$(OBJEXT)
	-rm -f src/collection/queue_lock_mutex.$(OBJEXT)
	-rm -f src/jsmn/jsmn.$(OBJEXT)

distclean-compile:
	-rm -f *.tab.c

include src/cluster/$(DEPDIR)/Cluster.Po
include src/cluster/$(DEPDIR)/ClusterUtil.Po
include src/cluster/$(DEPDIR)/NodeInfo.Po
include src/collection/$(DEPDIR)/hashset.Po
include src/collection/$(DEPDIR)/queue_lock_mutex.Po
include src/jsmn/$(DEPDIR)/jsmn.Po

.c.o:
	depbase=`echo $@ | sed 's|[^/]*$$|$(DEPDIR)/&|;s|\.o$$||'`;\
	$(COMPILE) -MT $@ -MD -MP -MF $$depbase.Tpo -c -o $@ $< &&\
	mv -f $$depbase.Tpo $$depbase.Po
#	source='$<' object='$@' libtool=no \
#	DEPDIR=$(DEPDIR) $(CCDEPMODE) $(depcomp) \
#	$(COMPILE) -c -o $@ $<

.c.obj:
	depbase=`echo $@ | sed 's|[^/]*$$|$(DEPDIR)/&|;s|\.obj$$||'`;\
	$(COMPILE) -MT $@ -MD -MP -MF $$depbase.Tpo -c -o $@ `$(CYGPATH_W) '$<'` &&\
	mv -f $$depbase.Tpo $$depbase.Po
#	source='$<' object='$@' libtool=no \
#	DEPDIR=$(DEPDIR) $(CCDEPMODE) $(depcomp) \
#	$(COMPILE) -c -o $@ `$(CYGPATH_W) '$<'`
install-includeHEADERS: $(include_HEADERS)
	@$(NORMAL_INSTALL)
	test -z "$(includedir)" || $(MKDIR_P) "$(DESTDIR)$(includedir)"
	@list='$(include_HEADERS)'; for p in $$list; do \
	  if test -f "$$p"; then d=; else d="$(srcdir)/"; fi; \
	  f=$(am__strip_dir) \
	  echo " $(includeHEADERS_INSTALL) '$$d$$p' '$(DESTDIR)$(includedir)/$$f'"; \
	  $(includeHEADERS_INSTALL) "$$d$$p" "$(DESTDIR)$(includedir)/$$f"; \
	done

uninstall-includeHEADERS:
	@$(NORMAL_UNINSTALL)
	@list='$(include_HEADERS)'; for p in $$list; do \
	  f=$(am__strip_dir) \
	  echo " rm -f '$(DESTDIR)$(includedir)/$$f'"; \
	  rm -f "$(DESTDIR)$(includedir)/$$f"; \
	done

ID: $(HEADERS) $(SOURCES) $(LISP) $(TAGS_FILES)
	list='$(SOURCES) $(HEADERS) $(LISP) $(TAGS_FILES)'; \
	unique=`for i in $$list; do \
	    if test -f "$$i"; then echo $$i; else echo $(srcdir)/$$i; fi; \
	  done | \
	  $(AWK) '    { files[$$0] = 1; } \
	       END { for (i in files) print i; }'`; \
	mkid -fID $$unique
tags: TAGS

TAGS:  $(HEADERS) $(SOURCES)  $(TAGS_DEPENDENCIES) \
		$(TAGS_FILES) $(LISP)
	tags=; \
	here=`pwd`; \
	list='$(SOURCES) $(HEADERS)  $(LISP) $(TAGS_FILES)'; \
	unique=`for i in $$list; do \
	    if test -f "$$i"; then echo $$i; else echo $(srcdir)/$$i; fi; \
	  done | \
	  $(AWK) '    { files[$$0] = 1; } \
	       END { for (i in files) print i; }'`; \
	if test -z "$(ETAGS_ARGS)$$tags$$unique"; then :; else \
	  test -n "$$unique" || unique=$$empty_fix; \
	  $(ETAGS) $(ETAGSFLAGS) $(AM_ETAGSFLAGS) $(ETAGS_ARGS) \
	    $$tags $$unique; \
	fi
ctags: CTAGS
CTAGS:  $(HEADERS) $(SOURCES)  $(TAGS_DEPENDENCIES) \
		$(TAGS_FILES) $(LISP)
	tags=; \
	here=`pwd`; \
	list='$(SOURCES) $(HEADERS)  $(LISP) $(TAGS_FILES)'; \
	unique=`for i in $$list; do \
	    if test -f "$$i"; then echo $$i; else echo $(srcdir)/$$i; fi; \
	  done | \
	  $(AWK) '    { files[$$0] = 1; } \
	       END { for (i in files) print i; }'`; \
	test -z "$(CTAGS_ARGS)$$tags$$unique" \
	  || $(CTAGS) $(CTAGSFLAGS) $(AM_CTAGSFLAGS) $(CTAGS_ARGS) \
	     $$tags $$unique

GTAGS:
	here=`$(am__cd) $(top_builddir) && pwd` \
	  && cd $(top_srcdir) \
	  && gtags -i $(GTAGS_ARGS) $$here

distclean-tags:
	-rm -f TAGS ID GTAGS GRTAGS GSYMS GPATH tags

distdir: $(DISTFILES)
	$(am__remove_distdir)
	test -d $(distdir) || mkdir $(distdir)
	@srcdirstrip=`echo "$(srcdir)" | sed 's/[].[^$$\\*]/\\\\&/g'`; \
	topsrcdirstrip=`echo "$(top_srcdir)" | sed 's/[].[^$$\\*]/\\\\&/g'`; \
	list='$(DISTFILES)'; \
	  dist_files=`for file in $$list; do echo $$file; done | \
	  sed -e "s|^$$srcdirstrip/||;t" \
	      -e "s|^$$topsrcdirstrip/|$(top_builddir)/|;t"`; \
	case $$dist_files in \
	  */*) $(MKDIR_P) `echo "$$dist_files" | \
			   sed '/\//!d;s|^|$(distdir)/|;s,/[^/]*$$,,' | \
			   sort -u` ;; \
	esac; \
	for file in $$dist_files; do \
	  if test -f $$file || test -d $$file; then d=.; else d=$(srcdir); fi; \
	  if test -d $$d/$$file; then \
	    dir=`echo "/$$file" | sed -e 's,/[^/]*$$,,'`; \
	    if test -d $(srcdir)/$$file && test $$d != $(srcdir); then \
	      cp -pR $(srcdir)/$$file $(distdir)$$dir || exit 1; \
	    fi; \
	    cp -pR $$d/$$file $(distdir)$$dir || exit 1; \
	  else \
	    test -f $(distdir)/$$file \
	    || cp -p $$d/$$file $(distdir)/$$file \
	    || exit 1; \
	  fi; \
	done
	-find $(distdir) -type d ! -perm -755 -exec chmod u+rwx,go+rx {} \; -o \
	  ! -type d ! -perm -444 -links 1 -exec chmod a+r {} \; -o \
	  ! -type d ! -perm -400 -exec chmod a+r {} \; -o \
	  ! -type d ! -perm -444 -exec $(install_sh) -c -m a+r {} {} \; \
	|| chmod -R a+r $(distdir)
dist-gzip: distdir
	tardir=$(distdir) && $(am__tar) | GZIP=$(GZIP_ENV) gzip -c >$(distdir).tar.gz
	$(am__remove_distdir)

dist-bzip2: distdir
	tardir=$(distdir) && $(am__tar) | bzip2 -9 -c >$(distdir).tar.bz2
	$(am__remove_distdir)

dist-tarZ: distdir
	tardir=$(distdir) && $(am__tar) | compress -c >$(distdir).tar.Z
	$(am__remove_distdir)

dist-shar: distdir
	shar $(distdir) | GZIP=$(GZIP_ENV) gzip -c >$(distdir).shar.gz
	$(am__remove_distdir)

dist-zip: distdir
	-rm -f $(distdir).zip
	zip -rq $(distdir).zip $(distdir)
	$(am__remove_distdir)

dist dist-all: distdir
	tardir=$(distdir) && $(am__tar) | GZIP=$(GZIP_ENV) gzip -c >$(distdir).tar.gz
	$(am__remove_distdir)

# This target untars the dist file and tries a VPATH configuration.  Then
# it guarantees that the distribution is self-contained by making another
# tarfile.
distcheck: dist
	case '$(DIST_ARCHIVES)' in \
	*.tar.gz*) \
	  GZIP=$(GZIP_ENV) gunzip -c $(distdir).tar.gz | $(am__untar) ;;\
	*.tar.bz2*) \
	  bunzip2 -c $(distdir).tar.bz2 | $(am__untar) ;;\
	*.tar.Z*) \
	  uncompress -c $(distdir).tar.Z | $(am__untar) ;;\
	*.shar.gz*) \
	  GZIP=$(GZIP_ENV) gunzip -c $(distdir).shar.gz | unshar ;;\
	*.zip*) \
	  unzip $(distdir).zip ;;\
	esac
	chmod -R a-w $(distdir); chmod a+w $(distdir)
	mkdir $(distdir)/_build
	mkdir $(distdir)/_inst
	chmod a-w $(distdir)
	dc_install_base=`$(am__cd) $(distdir)/_inst && pwd | sed -e 's,^[^:\\/]:[\\/],/,'` \
	  && dc_destdir="$${TMPDIR-/tmp}/am-dc-$$$$/" \
	  && cd $(distdir)/_build \
	  && ../configure --srcdir=.. --prefix="$$dc_install_base" \
	    $(DISTCHECK_CONFIGURE_FLAGS) \
	  && $(MAKE) $(AM_MAKEFLAGS) \
	  && $(MAKE) $(AM_MAKEFLAGS) dvi \
	  && $(MAKE) $(AM_MAKEFLAGS) check \
	  && $(MAKE) $(AM_MAKEFLAGS) install \
	  && $(MAKE) $(AM_MAKEFLAGS) installcheck \
	  && $(MAKE) $(AM_MAKEFLAGS) uninstall \
	  && $(MAKE) $(AM_MAKEFLAGS) distuninstallcheck_dir="$$dc_install_base" \
	        distuninstallcheck \
	  && chmod -R a-w "$$dc_install_base" \
	  && ({ \
	       (cd ../.. && umask 077 && mkdir "$$dc_destdir") \
	       && $(MAKE) $(AM_MAKEFLAGS) DESTDIR="$$dc_destdir" install \
	       && $(MAKE) $(AM_MAKEFLAGS) DESTDIR="$$dc_destdir" uninstall \
	       && $(MAKE) $(AM_MAKEFLAGS) DESTDIR="$$dc_destdir" \
	            distuninstallcheck_dir="$$dc_destdir" distuninstallcheck; \
	      } || { rm -rf "$$dc_destdir"; exit 1; }) \
	  && rm -rf "$$dc_destdir" \
	  && $(MAKE) $(AM_MAKEFLAGS) dist \
	  && rm -rf $(DIST_ARCHIVES) \
	  && $(MAKE) $(AM_MAKEFLAGS) distcleancheck
	$(am__remove_distdir)
	@(echo "$(distdir) archives ready for distribution: "; \
	  list='$(DIST_ARCHIVES)'; for i in $$list; do echo $$i; done) | \
	  sed -e 1h -e 1s/./=/g -e 1p -e 1x -e '$$p' -e '$$x'
distuninstallcheck:
	@cd $(distuninstallcheck_dir) \
	&& test `$(distuninstallcheck_listfiles) | wc -l` -le 1 \
	   || { echo "ERROR: files left after uninstall:" ; \
	        if test -n "$(DESTDIR)"; then \
	          echo "  (check DESTDIR support)"; \
	        fi ; \
	        $(distuninstallcheck_listfiles) ; \
	        exit 1; } >&2
distcleancheck: distclean
	@if test '$(srcdir)' = . ; then \
	  echo "ERROR: distcleancheck can only run from a VPATH build" ; \
	  exit 1 ; \
	fi
	@test `$(distcleancheck_listfiles) | wc -l` -eq 0 \
	  || { echo "ERROR: files left in build directory after distclean:" ; \
	       $(distcleancheck_listfiles) ; \
	       exit 1; } >&2
check-am: all-am
check: check-am
all-am: Makefile $(LIBRARIES) $(HEADERS)
installdirs:
	for dir in "$(DESTDIR)$(libdir)" "$(DESTDIR)$(includedir)"; do \
	  test -z "$$dir" || $(MKDIR_P) "$$dir"; \
	done
install: install-am
install-exec: install-exec-am
install-data: install-data-am
uninstall: uninstall-am

install-am: all-am
	@$(MAKE) $(AM_MAKEFLAGS) install-exec-am install-data-am

installcheck: installcheck-am
install-strip:
	$(MAKE) $(AM_MAKEFLAGS) INSTALL_PROGRAM="$(INSTALL_STRIP_PROGRAM)" \
	  install_sh_PROGRAM="$(INSTALL_STRIP_PROGRAM)" INSTALL_STRIP_FLAG=-s \
	  `test -z '$(STRIP)' || \
	    echo "INSTALL_PROGRAM_ENV=STRIPPROG='$(STRIP)'"` install
mostlyclean-generic:

clean-generic:

distclean-generic:
	-test -z "$(CONFIG_CLEAN_FILES)" || rm -f $(CONFIG_CLEAN_FILES)
	-rm -f src/cluster/$(DEPDIR)/$(am__dirstamp)
	-rm -f src/cluster/$(am__dirstamp)
	-rm -f src/collection/$(DEPDIR)/$(am__dirstamp)
	-rm -f src/collection/$(am__dirstamp)
	-rm -f src/jsmn/$(DEPDIR)/$(am__dirstamp)
	-rm -f src/jsmn/$(am__dirstamp)

maintainer-clean-generic:
	@echo "This command is intended for maintainers to use"
	@echo "it deletes files that may require special tools to rebuild."
clean: clean-am

clean-am: clean-generic clean-libLIBRARIES mostlyclean-am

distclean: distclean-am
	-rm -f $(am__CONFIG_DISTCLEAN_FILES)
	-rm -rf src/cluster/$(DEPDIR) src/collection/$(DEPDIR) src/jsmn/$(DEPDIR)
	-rm -f Makefile
distclean-am: clean-am distclean-compile distclean-generic \
	distclean-tags

dvi: dvi-am

dvi-am:

html: html-am

info: info-am

info-am:

install-data-am: install-includeHEADERS

install-dvi: install-dvi-am

install-exec-am: install-libLIBRARIES

install-html: install-html-am

install-info: install-info-am

install-man:

install-pdf: install-pdf-am

install-ps: install-ps-am

installcheck-am:

maintainer-clean: maintainer-clean-am
	-rm -f $(am__CONFIG_DISTCLEAN_FILES)
	-rm -rf $(top_srcdir)/autom4te.cache
	-rm -rf src/cluster/$(DEPDIR) src/collection/$(DEPDIR) src/jsmn/$(DEPDIR)
	-rm -f Makefile
maintainer-clean-am: distclean-am maintainer-clean-generic

mostlyclean: mostlyclean-am

mostlyclean-am: mostlyclean-compile mostlyclean-generic

pdf: pdf-am

pdf-am:

ps: ps-am

ps-am:

uninstall-am: uninstall-includeHEADERS uninstall-libLIBRARIES

.MAKE: install-am install-strip

.PHONY: CTAGS GTAGS all all-am am--refresh check check-am clean \
	clean-generic clean-libLIBRARIES ctags dist dist-all \
	dist-bzip2 dist-gzip dist-shar dist-tarZ dist-zip distcheck \
	distclean distclean-compile distclean-generic distclean-tags \
	distcleancheck distdir distuninstallcheck dvi dvi-am html \
	html-am info info-am install install-am install-data \
	install-data-am install-dvi install-dvi-am install-exec \
	install-exec-am install-html install-html-am \
	install-includeHEADERS install-info install-info-am \
	install-libLIBRARIES install-man install-pdf install-pdf-am \
	install-ps install-ps-am install-strip installcheck \
	installcheck-am installdirs maintainer-clean \
	maintainer-clean-generic mostlyclean mostlyclean-compile \
	mostlyclean-generic pdf pdf-am ps ps-am tags uninstall \
	uninstall-am uninstall-includeHEADERS uninstall-libLIBRARIES

# Tell versions [3.59,3.63) of GNU make to not export all variables.
# Otherwise a system limit (for SysV at least) may be exceeded.
.NOEXPORT:
