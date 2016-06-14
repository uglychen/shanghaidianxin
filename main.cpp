/*
*    author: chenxun
*/

#include <iostream>
#include <set>
#include <string>
#include <pthread.h>

#include "json_data.h"
#include "KafkaWrapper.h"

using namespace std;


#define NUM_THREADS 3

int main(int argc, char* argv[])
{
	
	pthread_t tid[10] = {0};
  	int code;
  	unsigned int i;
 
 	for (i = 0; i < 10; i++)
  	{
    	code = pthread_create(&tid[i], NULL, thread_function, (void*)i);
    	if (code != 0)
    	{
      		fprintf(stderr, "Create new thread failed: %s\n", strerror(code));
     		exit(1);
    	}
    	fprintf(stdout, "New thread created.\n");
  	}

	for (i = 0; i < 10; i++)
	{
		pthread_join(tid[i],NULL);
		fprintf(stderr, "Join thread 1 error: %s\n", strerror(code));
		exit(1);
	}

	return 0;
}


