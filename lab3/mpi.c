#include "utility.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <mpi.h>
#include <math.h>
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

  unsigned long int *numbers;

  int myrank, proccount;
  unsigned long result = 0, *resulttemp;
  int sentcount = 0, recvcount = 0;
  int requestcompleted = 0;
  int i;
  MPI_Status status;
  unsigned long int number;
  MPI_Request *requests;


  MPI_Init(&argc,&argv);

  // obtain my rank
  MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
  // and the number of processes
  MPI_Comm_size(MPI_COMM_WORLD,&proccount);

  if(!myrank){
    gettimeofday(&ins__tstart, NULL);
	  numbers = (unsigned long int*)malloc(inputArgument * sizeof(unsigned long int));
  	numgen(inputArgument, numbers);
  }

  // run your computations here (including MPI communication)
  if(proccount < 2) {
    printf("Run with at least 2 processes");
    MPI_Finalize();
    return -1;
  }

  // now the master will distribute the data and slave processes will perform computations
  if (myrank == 0) {
    // i = 1,2,3
    // numbers 0,1,2
    // first distribute some ranges to all slaves
    for (int i = 1; i < proccount; i++) {
      // send it to process i
      MPI_Send(&numbers[i - 1], 1, MPI_UNSIGNED_LONG, i, DATA, MPI_COMM_WORLD);
      sentcount++;
    }


    requests = (MPI_Request *) malloc (3 * (proccount - 1) * sizeof (MPI_Request));

    for(int i = 0; i < 2 * (proccount - 1); i++) {
      requests[i] = MPI_REQUEST_NULL;
    }

    resulttemp = (unsigned long *) malloc ((proccount - 1) * sizeof (unsigned long));

    for (int i = 1; i<proccount; i++) {
      MPI_Irecv(&resulttemp[i-1], 1, MPI_UNSIGNED_LONG, MPI_ANY_SOURCE, RESULT, MPI_COMM_WORLD, &requests[i-1]);
    }

    for (int i = 1; i<proccount; i++) {
      MPI_Isend(numbers[sentcount+i-1],1,MPI_UNSIGNED_LONG,i,DATA,MPI_COMM_WORLD,&(requests[proccount-2+i])); 
      sentcount++;
    }
    
    do {
      MPI_Waitany(2 * proccount - 2, requests, &requestcompleted, MPI_STATUS_IGNORE);

      // distribute remaining subranges to the processes which have completed their parts
      //MPI_Recv(&resulttemp, 1, MPI_UNSIGNED_LONG, MPI_ANY_SOURCE, RESULT, MPI_COMM_WORLD, &status);
      //result += resulttemp;
      /// numbers 2,3,4
      //MPI_Send(&numbers[sentcount - 1], 1, MPI_UNSIGNED_LONG, status.MPI_SOURCE, DATA, MPI_COMM_WORLD);
      
      if (requestcompleted < (proccount - 1)) {
        result += resulttemp[requestcompleted];
        
        MPI_Wait (&(requests[proccount - 1 + requestcompleted]), MPI_STATUS_IGNORE);
        sentcount++;

        MPI_Irecv (&(resulttemp[requestcompleted]), 1,
				MPI_UNSIGNED_LONG, requestcompleted + 1, RESULT,
				MPI_COMM_WORLD,
				&(requests[requestcompleted]));
	    }

    
    }
    while (sentcount < inputArgument);
      
    sentcount++;
    

    // now receive results from the processes
    for (int i = 0; i < (proccount - 1); i++) {
      MPI_Recv(&resulttemp, 1, MPI_UNSIGNED_LONG, MPI_ANY_SOURCE, RESULT, MPI_COMM_WORLD, &status);
      result += resulttemp;
    }
    // shut down the slaves
    for (int i = 1; i < proccount; i++) {
      MPI_Send(NULL, 0, MPI_UNSIGNED_LONG, i, FINISH, MPI_COMM_WORLD);
    }
    // now display the result
    printf("\nHi, I am process 0, the result is %ld\n", (inputArgument - result));

  }
  else {				// slave
          // this is easy - just receive data and do the work
    do {
      MPI_Probe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

      if (status.MPI_TAG == DATA) {
        MPI_Recv(&number, 2, MPI_UNSIGNED_LONG, 0, DATA, MPI_COMM_WORLD, &status);
        // compute my part
        resulttemp = 0;
        //sprawdz czy pierwsza
        
        for (unsigned long i = 2; i < sqrt(number) + 1; i += 1) {
          if (number % i == 0) {
            resulttemp = 1;
            break;
          }
        }

        // send the result back
        MPI_Send(&resulttemp, 1, MPI_UNSIGNED_LONG, 0, RESULT, MPI_COMM_WORLD);
      }
    }
    while (status.MPI_TAG != FINISH);
  }




  // synchronize/finalize your computations

  if (!myrank) {
    gettimeofday(&ins__tstop, NULL);
    ins__printtime(&ins__tstart, &ins__tstop, ins__args.marker);
  }
  
  MPI_Finalize();

}

// #include "utility.h"
// #include <stdio.h>
// #include <stdlib.h>
// #include <sys/time.h>
// #include <mpi.h>
// #include "numgen.c"

// #define DATA 0
// #define RESULT 1
// #define FINISH 2

// int is_prime(long n)
// {
//   if (n < 2)
//     return 0;

//   for (long i = 2; i*i <= n; i++)
//   {
//     if (n % i == 0)
//       return 0;
//   }

//   return 1;
// }

// int main(int argc,char **argv) 
// {

//   Args ins__args;
//   parseArgs(&ins__args, &argc, argv);

//   //program input argument
//   long inputArgument = ins__args.arg; 

//   struct timeval ins__tstart, ins__tstop;

//   int myrank,nproc;
//   unsigned long int *numbers;
//   unsigned long int result = 0, length = inputArgument, resulttemp;
//   MPI_Status status;
//   MPI_Request *requests;

//   MPI_Init(&argc,&argv);

//   // obtain my rank
//   MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
//   // and the number of processes
//   MPI_Comm_size(MPI_COMM_WORLD,&nproc);

//   while (length % (nproc - 1)) length++;

//   if(!myrank)
//   {
//     gettimeofday(&ins__tstart, NULL);
// 	  numbers = (unsigned long int*)calloc(length, sizeof(unsigned long int));
//   	numgen(inputArgument, numbers);
//   }

//   // run your computations here (including MPI communication)
//   unsigned long range_size = length / (nproc - 1);

//   requests = (MPI_Request *) malloc ((nproc - 1) * sizeof (MPI_Request));

//   for (int i = 0; i < nproc - 1; i++)
//       requests[i] = MPI_REQUEST_NULL;

//   if (myrank == 0)
//   {
//     //printf("Array:\n");
//     // for (int i = 0; i < length; i++)
//     // {
//     //     printf("%lu\n", numbers[i]);
//     // }

//     unsigned long start = 0;
//     for (int i = 1; i < nproc; i++)
//     {
//       MPI_Isend (numbers + start, range_size, MPI_UNSIGNED_LONG, i, DATA, MPI_COMM_WORLD, &(requests[i - 1]));
//       start += range_size;
//     }

//     // now receive results from the processes
//     for (int i = 0; i < (nproc- 1); i++)
//     {
//         MPI_Recv (&resulttemp, 1, MPI_UNSIGNED_LONG, MPI_ANY_SOURCE, RESULT, MPI_COMM_WORLD, &status);
//         result += resulttemp;
//     }
    
//     printf ("\nPrime numbers: %lu\n", result);
//   }
//   // slave 
//   else
//   {
//       MPI_Probe (0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
// 	    if (status.MPI_TAG == DATA)
// 	    {

//         numbers = (unsigned long int*)malloc(range_size * sizeof(unsigned long int));

//         MPI_Irecv (numbers, range_size, MPI_UNSIGNED_LONG, 0, DATA, MPI_COMM_WORLD, &(requests[myrank - 1]));
       
//         for (int i = 0; i < range_size; i++)
//         {
//           //printf("Process %d is checking number %lu\n", myrank, *(numbers + i));
//           result += is_prime(*(numbers + i));
//         }

//         // send the result back
//         MPI_Isend (&result, 1, MPI_UNSIGNED_LONG, 0, RESULT, MPI_COMM_WORLD, &(requests[myrank - 1]));
//       }
//   }


//   // synchronize/finalize your computations

//   if (!myrank) {
//     gettimeofday(&ins__tstop, NULL);
//     ins__printtime(&ins__tstart, &ins__tstop, ins__args.marker);
//   }

//   free(numbers);
//   MPI_Finalize();

// }

