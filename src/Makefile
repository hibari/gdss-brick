###----------------------------------------------------------------------
### Copyright (c) 2007-2010 Gemini Mobile Technologies, Inc.  All rights reserved.
###
### Licensed under the Apache License, Version 2.0 (the "License");
### you may not use this file except in compliance with the License.
### You may obtain a copy of the License at
###
###     http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.
###
### File    : Makefile
### Purpose : 
###----------------------------------------------------------------------

# Most of this Makefile is boilerplate ... except the following two symbols.

APPNAME1 = gdss
NODENAME1 = gdss_dev2
MODULES1 = \
	brick brick_sup \
	brick_admin brick_admin_event_h brick_admin_sup \
	brick_bp brick_pingee brick_chainmon brick_cinfo brick_clientmon \
	brick_brick_sup brick_data_sup \
	brick_hash brick_mboxmon brick_migmon brick_mon_sup \
	brick_sb \
	brick_server brick_shepherd brick_simple \
	brick_ets \
	brick_squorum brick_sub_sup brick_itimer \
	brick_ticket \
	web_admin_sup mod_admin \
	gmt_hlog gmt_hlog_local gmt_hlog_common

APPNAME2 = gdss_client
NODENAME2 = gdss_client_dev2
MODULES2 = \
	brick_client brick_client_sup \
	brick_client_data_sup

APPNAME3 = gdss
NODENAME3 = gdss_dev3
MODULES3 = $(MODULES1)

APPNAME4 = gdss
NODENAME4 = gdss_dev4
MODULES4 = $(MODULES1)

TESTS = \
	brick_test0 \
	test_reliablefifo \
	test_xref \
	test_scav \
	brick_itimer_test

EUNITTESTS = \
	brick_ets_test

DISABLEDQUICKTESTS = \
	hash_qc \
	hlog_qc hlog_local_qc hlog_qc_blackbox \
	my_pread_qc \
	repair_qc \
	simple_qc \
	simple_qc_offline \
	squorum_qc \
	admin_qc

# include erl make
include $(shell cat ../.depends-gmt-bom)/make/erl_make.mk

# standard targets
clean::
	rm -f core core.* *.core erl_crash.dump *.log
	rm -rf *.log *.LOG hlog.* Schema.bootstrap Schema.local Schema.syncml

check: all
	@echo ""
	@echo "NOTE: Longer-running regression tests should be run using"
	@echo "      'make check-long'."
	@echo ""
	@echo "NOTE: Longer-running QuickCheck tests should be run using"
	@echo "      'make check-quickcheck'."
	@echo ""
	@sleep 2
	$(RUNAPP1) -noshell -noinput \
		-s brick_squorum t0 \
		-s brick_test0 cl_chain_t35 \
		-s brick_test0 cl_single_brick_regression \
		-s erlang halt > brick_test0.log

check-long: all
	$(RUNAPP1) -noshell -noinput \
		-s brick_test0 cl_chain_all \
		-s erlang halt > brick_test0.log

check-iterm: all
	$(RUNERL1) -noshell -noinput \
		-s brick_itimer_test start_test \
		-s erlang halt > itimer.log

chk: all
	rm -rf hlog.*
	rm -f *.log
	$(RUNAPP1) -noshell -noinput \
		-s test_reliablefifo brick0 \
		-s erlang halt > chk1.log & 
	$(RUNAPP3) -noshell -noinput \
		-s test_reliablefifo brick0 \
		-s erlang halt > chk2.log & 
	$(RUNAPP4) -noshell -noinput \
		-s test_reliablefifo cl_chain_reliablefifo \
		-s erlang halt | tee chk3.log

chk2: all
	rm -rf hlog.*
	rm -f *.log testtableschema.txt
	$(RUNAPP1) -noshell -noinput \
		-s test_scav test_all \
		-s erlang halt > chk2.log &
	$(RUNAPP3) -noshell -noinput \
		-s test_scav get_busy \
		-s erlang halt |tee chk3.log

check-quickcheck: check-quickcheck-pretest \
    check-qc-hlog-local-qc check-qc-hlog-qc1 \
    check-qc-hlog-qc-blackbox check-qc-my-pread-qc check-qc-repair-qc2 \
    check-qc-simple-qc
	@echo ""
	@echo All QuickCheck tests pass.
	@echo ""

check-quickcheck-pretest:
	@echo ""
	@echo "NOTE: The following QuickCheck tests are disabled due to"
	@echo "      false positives or other model problems:"
	@echo "         check-qc-hlog-qc2 (qc_hlog.erl)"
	@echo "         check-qc-squorum-qc (squorum_qc.erl)"
	@echo ""
	@echo "NOTE: On Hibari's machine, this longer suite of QuickCheck tests,"
	@echo "      at the default 500 iterations per test type, requires"
	@echo "      approximately 40 minutes to run."
	@echo ""
	@echo "WARNING: These tests may destroy hlog.* data dirs, schema files,"
	@echo "         and perhaps other data.  Press Control-c now to"
	@echo "         avoid such data deletion."
	@echo ""
	@sleep 10

check-qc-hlog-local-qc: all
	$(RUNAPP1) -noshell -noinput \
		-s brick_test0 cl_qc_hlog_local_qc__t1 $(QC_NUMTESTS) \
		-s erlang halt 2>&1 | tee brick_qc.log

check-qc-hlog-qc1: all
	$(RUNAPP1) -noshell -noinput \
		-s brick_test0 cl_qc_hlog_qc__t1 $(QC_NUMTESTS) \
		-s erlang halt 2>&1 | tee brick_qc.log

# NOTE: This QC model can fail with false positives (scribble over the
#       length byte of the last hunk_summ header in a file will not
#       cause a failure).  Do not use until fixed.

check-qc-hlog-qc2: all
	$(RUNAPP1) -noshell -noinput \
		-s brick_test0 cl_qc_hlog_qc__t2 $(QC_NUMTESTS) \
		-s erlang halt 2>&1 | tee brick_qc.log

check-qc-hlog-qc-blackbox: all
	$(RUNAPP1) -noshell -noinput \
		-s brick_test0 cl_qc_hlog_qc_blackbox__t1 $(QC_NUMTESTS) \
		-s erlang halt 2>&1 | tee brick_qc.log

check-qc-my-pread-qc: all
	$(RUNAPP1) -noshell -noinput \
		-s brick_test0 cl_qc_my_pread_qc__t1 $(QC_NUMTESTS) \
		-s erlang halt 2>&1 | tee brick_qc.log

# NOTE: We won't normally run check-qc-repair-qc1 because it's probably
#       sufficient to call check-qc-repair-qc2 a superset of the testing
#       that the check-qc-repair-qc1 target will do.

check-qc-repair-qc1: all
	$(RUNAPP1) -noshell -noinput \
		-s brick_test0 cl_qc_repair_qc__t1 $(QC_NUMTESTS) \
		-s erlang halt 2>&1 | tee brick_qc.log

check-qc-repair-qc2: all
	$(RUNAPP1) -noshell -noinput \
		-s brick_test0 cl_qc_repair_qc__t2 $(QC_NUMTESTS) \
		-s erlang halt 2>&1 | tee brick_qc.log

check-qc-simple-qc: all
	$(RUNAPP1) -noshell -noinput \
		-s brick_test0 cl_qc_simple_qc__t1 $(QC_NUMTESTS) \
		-s erlang halt 2>&1 | tee brick_qc.log

# NOTE: This QC model can fail with false positives (deleting more
#       copies than quorum size).
#       Do not use until fixed.

check-qc-squorum-qc: all
	$(RUNAPP1) -noshell -noinput \
		-s brick_test0 cl_qc_hlog_qc__t2 $(QC_NUMTESTS) \
		-s erlang halt 2>&1 | tee brick_qc.log

interactive:
	$(RUNAPP1) -pz Unit-EUnit-Files
