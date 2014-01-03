PACKAGE_NAME = MessagingDNSLookup
BRAZIL_PACKAGE_VERSION?=1.0
PACKAGE_SUBDIRECTORY = shared/platform/MessagingDNSLookup

all:

# Determine global tools location
APOLLO_TOOLS_BASE=/apollo/env/SDETools
OPT_AMAZON_BASE=/opt/amazon/platform-services/etc/BrazilTools-2.1
GLOBAL_TOOLS_BASE=$(if $(wildcard $(APOLLO_TOOLS_BASE)/bin/findup),$(APOLLO_TOOLS_BASE),$(OPT_AMAZON_BASE))
GLOBAL_TOOLS_BIN:=$(if $(wildcard $(GLOBAL_TOOLS_BASE)/bin/findup),$(GLOBAL_TOOLS_BASE)/bin,$(GLOBAL_TOOLS_BASE)/bin/DEV.STD.PTHREAD)

BRAZIL_HOME := $(shell $(GLOBAL_TOOLS_BIN)/findup make-support)
include $(BRAZIL_HOME)/make-support/bootstrap.mk


JFLAGS += -g -source 1.5

JAVA_BASE = $(JDK_INSTALL_HOME)/jdk1.5.0
JAVAHOME=$(JAVA_BASE)
JAVA_HOME=$(JAVAHOME)
JAVA_LIB_PATH = $(JAVA_BASE)/lib
JAVA_JAVA_PATH = $(JAVA_BASE)/jre/lib
JAVA_CLASSPATH = $(JAVA_JAVA_PATH)/rt.jar
JAVA_INCLUDE_PATH = $(JAVA_BASE)/include:$(JAVA_BASE)/include/linux
JAVA_INCLUDE = -I$(JAVA_BASE)/include -I$(JAVA_BASE)/include/linux
