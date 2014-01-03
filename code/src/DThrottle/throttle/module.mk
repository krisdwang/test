THROTTLE_OBJECTS = \
	throttle/throttle.o \
	throttle/throttleapp.o \
	throttle/socketreader.o \
	throttle/FLSocket2.o \
	$(NULL)

THROTTLE_TEST_OBJECTS = \
	throttle/tests/test.o \
	throttle/tests/comm_test.o \
	$(NULL)

THROTTLE_TEST_TARGETS = test comm_test 

