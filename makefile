ALL= run_shanghai
LIB= -lcurl -lrdkafka -lpthread -lrt 
Include = -I/usr/local/include/librdkafka
Flags = -g -O2 -fPIC -Wall -Wsign-compare -Wfloat-equal -Wpointer-arith

objects= main.o json_data.o json_value.o json_writer.o json_reader.o  basictypes.o base64.o KafkaWrapper.o

$(ALL) : $(objects)
	g++ -o   $(Iclude) $@ $(objects) $(LIB)
%.o:%.cpp
	g++  -c $(Include)  $< -o $@
.PHONY : clean

clean:
	rm $(objects)

