#include "utility.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <mpi.h>
#include "numgen.c"

#define DATA 0
#define RESULT 1
#define FINISH 2

int main(int argc,char **argv) {

  Args ins__args;
  parseArgs(&ins__args, &argc, argv);

  //program input argument
  long inputArgument = ins__args.arg; 

  struct timeval ins__tstart, ins__tstop;

  int myrank,nproc;
  unsigned long int *numbers;

  MPI_Init(&argc,&argv);

  // obtain my rank
  MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
  // and the number of processes
  MPI_Comm_size(MPI_COMM_WORLD,&nproc);

  if(!myrank){
      	gettimeofday(&ins__tstart, NULL);
	numbers = (unsigned long int*)malloc(inputArgument * sizeof(unsigned long int));
  	numgen(inputArgument, numbers);
  }

  // run your computations here (including MPI communication)
  // dla 200 liczb -> 10 prime liczb
  long numbersLength = inputArgument;
  long currentNumber = 0;
  int isEvenTemp;
  long evenNumbersLength = 0;
  unsigned long int number;
  unsigned long int receivedNumber;

  MPI_Status status;

  MPI_Status* statuses;
  MPI_Request* requests;
  int* resultsTemp;
  int requestCompleted;
  int flag;

  if (nproc < 2)
  {
        printf ("Run with at least 2 processes");
        MPI_Finalize ();
        return -1;
  }
  if (numbersLength < nproc)
  {
      printf ("More numbers needed");
      MPI_Finalize ();
      return -1;
  }
  if (!myrank) { // MASTER PROCESS
    requests = (MPI_Request*) malloc(3 * (nproc - 1) * sizeof (MPI_Request));
    if (!requests)
    {
      printf ("\nNot enough memory");
	    MPI_Finalize ();
	    return -1;
    }
    resultsTemp = (int*) malloc((nproc - 1) * sizeof (int));
    if (!requests)
    {
      printf ("\nNot enough memory");
	    MPI_Finalize ();
	    return -1;
    }
    for (int i = 1; i < nproc; i++) {
      MPI_Send(&numbers[currentNumber++], 1, MPI_UNSIGNED_LONG, i, DATA, MPI_COMM_WORLD);
    }
    // the first proccount requests will be for receiving, the latter ones for sending
	  for (int i = 0; i < 2 * (nproc - 1); i++) {
  	  requests[i] = MPI_REQUEST_NULL;	// none active at this point
    }
    // start receiving for results from the slaves
	  for (int i = 1; i < nproc; i++) {
      MPI_Irecv (&(resultsTemp[i - 1]), 1, MPI_INT, i, RESULT, MPI_COMM_WORLD, &(requests[i - 1]));
    }
    for (int i = 1; i < nproc; i++) {
      MPI_Isend(&numbers[currentNumber++], 1, MPI_UNSIGNED_LONG, i, DATA, MPI_COMM_WORLD, &(requests[nproc - 2 + i]));
    }
    while (currentNumber < numbersLength) {
      // wait for completion of any of the requests
	    //MPI_Waitany(2 * nproc - 2, requests, &requestCompleted, MPI_STATUS_IGNORE);
      MPI_Testany(2 * nproc - 2, requests, &requestCompleted, &flag, MPI_STATUS_IGNORE);
      if (!flag) { // all processes are busy
        number = numbers[currentNumber++];
        isEvenTemp = 1;
        if (number == 0 || number == 1) {
          isEvenTemp = 0;
        }
        else if (number == 2) {
          isEvenTemp = 1;
        }
        else {
          for (unsigned long int i=2; i*i <=number; i++) {
            if (number % i == 0) {
              isEvenTemp = 0;
              break;
            }
          }
        }
        evenNumbersLength += isEvenTemp;
      }
      // if it is a result then send new data to the process
	    // and add the result
      if (flag && requestCompleted < (nproc - 1)) {
        evenNumbersLength += resultsTemp[requestCompleted];
        // first check if the send has terminated
		    MPI_Wait (&(requests[nproc - 1 + requestCompleted]), MPI_STATUS_IGNORE);
        MPI_Isend(&numbers[currentNumber++], 1, MPI_UNSIGNED_LONG, requestCompleted + 1, DATA, MPI_COMM_WORLD, &(requests[nproc - 1 + requestCompleted]));
        MPI_Irecv (&(resultsTemp[requestCompleted]), 1, MPI_INT, requestCompleted + 1, RESULT, MPI_COMM_WORLD, &(requests[requestCompleted]));
	    }
    }
    // Sending finish messages now
    for (int i = 1; i < nproc; i++) {
      MPI_Isend(&numbers[0], 0, MPI_UNSIGNED_LONG, i, DATA, MPI_COMM_WORLD, &(requests[2 * nproc - 3 + i]));
    }
    // now receive results from the processes - that is finalize the pending requests
    MPI_Waitall (3 * nproc - 3, requests, MPI_STATUSES_IGNORE);
    // now simply add the results
    for (int i = 0; i < (nproc - 1); i++) {
      evenNumbersLength += resultsTemp[i];
    }
    // now receive results for the initial sends
    for (int i = 0; i < (nproc - 1); i++) {
      MPI_Recv (&(resultsTemp[i]), 1, MPI_INT, i + 1, RESULT, MPI_COMM_WORLD, &status);
	    evenNumbersLength += resultsTemp[i];
    }
    printf("From generated numbers %ld are prime numbers", evenNumbersLength);
  }
  else {  // SLAVES PROCESS
    int finishedFlag = 0;
    statuses = (MPI_Status*) malloc(2 * sizeof (MPI_Status));
    if (!statuses)
    {
      printf ("\nNot enough memory");
	    MPI_Finalize ();
	    return -1;
    }
    requests = (MPI_Request*) malloc(2 * sizeof (MPI_Request));
    if (!requests)
    {
      printf ("\nNot enough memory");
	    MPI_Finalize ();
	    return -1;
    }
    requests[0] = requests[1] = MPI_REQUEST_NULL;

    resultsTemp = (int *) malloc (2 * sizeof (int));
    if (!resultsTemp)
    {
      printf ("\nNot enough memory");
      MPI_Finalize ();
      return -1;
    }

    MPI_Recv (&number, 1, MPI_UNSIGNED_LONG, 0, DATA, MPI_COMM_WORLD, &status);
    while (!finishedFlag)
    {
      MPI_Irecv (&receivedNumber, 1, MPI_UNSIGNED_LONG, 0, DATA, MPI_COMM_WORLD, &(requests[0]));
      // Calculating if it is prime number
      isEvenTemp = 1;
        if (number == 0 || number == 1) {
          isEvenTemp = 0;
        }
        else if (number == 2) {
          isEvenTemp = 1;
        }
        else {
          for (unsigned long int i=2; i*i <=number; i++) {
            if (number % i == 0) {
              isEvenTemp = 0;
              break;
            }
          }
        }
      resultsTemp[1] = isEvenTemp;

      MPI_Waitall (2, requests, statuses);

      int count; // if message is empty - it means that process should finish
      MPI_Get_count(&statuses[0], MPI_INT, &count);
      if(!count) {
        finishedFlag = 1;
      }

      number = receivedNumber;
      resultsTemp[0] = resultsTemp[1];
      MPI_Isend (&resultsTemp[0], 1, MPI_INT, 0, RESULT, MPI_COMM_WORLD, &(requests[1]));
    }
    MPI_Wait (&(requests[1]), MPI_STATUS_IGNORE);
  }
  // synchronize/finalize your computations

  if (!myrank) {
    gettimeofday(&ins__tstop, NULL);
    ins__printtime(&ins__tstart, &ins__tstop, ins__args.marker);
  }
  
  MPI_Finalize();

}
