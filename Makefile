# $Id: Makefile 10289 2013-01-07 17:38:26Z mgower $
# $Rev:: 10289                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit. 
# $LastChangedDate:: 2013-01-07 11:38:26 #$:  # Date of last commit.

SHELL=/bin/sh

build:
	@echo "QCFramework: Ready to install"

install: 
ifndef INSTALL_ROOT
	@echo "QCFramework: Must define INSTALL_ROOT"
	false
endif
	@echo "QCFramework: Installing to ${INSTALL_ROOT}"
	-mkdir -p ${INSTALL_ROOT}
	-rsync -Caq bin ${INSTALL_ROOT}
	-mkdir -p ${INSTALL_ROOT}/perl
	-rsync -Caq lib/QCF ${INSTALL_ROOT}/perl
	@echo "Make sure ${INSTALL_ROOT}/perl is in PERL5LIB"

test:
	@echo "QCFramework: tests are currently not available"

clean:
	rm -f  *~ \#*\#
