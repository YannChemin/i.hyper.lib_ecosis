ifeq ($(DEBIAN_BUILD),1)
PREFIX   ?= /usr/local
BINDIR    = $(DESTDIR)$(PREFIX)/bin
PGM_PY   = $(PGM).py

$(PGM): $(PGM_PY)
	cp $< $@ && chmod 755 $@

install: $(PGM)
	install -d $(BINDIR)
	install -m 755 $(PGM) $(BINDIR)/$(PGM)

clean:
	rm -f $(PGM)

.PHONY: install clean

else
MODULE_TOPDIR = ../..

PGM = i.hyper.lib_ecosis

include $(MODULE_TOPDIR)/include/Make/Script.make
include $(MODULE_TOPDIR)/include/Make/Html.make

default: script html $(TEST_DST)

# Ensure HTML manual is installed
$(HTMLDIR)/$(PGM).html: $(PGM).html
	$(INSTALL_DATA) $(PGM).html $(HTMLDIR)/$(PGM).html
endif
