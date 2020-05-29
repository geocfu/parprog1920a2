// code base for various sorting algorithms
// compile with e.g. gcc -O2 -Wall -pthread quicksort.c -o quicksort -DTHREADS=4
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

#include <pthread.h>

#define N 10000000
#define CREATE_LIMIT 100
#define QUEUE_SIZE N

// conditional variable, signals array put operation (receiver waits on this)
pthread_cond_t messageIn = PTHREAD_COND_INITIALIZER;
// conditional variable, signals array get operation (sender waits on this)
pthread_cond_t messageOut = PTHREAD_COND_INITIALIZER;
// mutex protecting common resources
pthread_mutex_t queueMutex = PTHREAD_MUTEX_INITIALIZER;

// The message schema
struct MessageParams {
    double *startPosition;
    int size;
    int terminationFlag;
    int shutdownFlag;
};

// queue size
int queueSize = 0;

int queueRear = -1;
int queueFront = 0;
// The queue that will hold the messages
struct MessageParams mParam[QUEUE_SIZE];

// Simple sorting algorithm so sort the remaining elements in the array
void inssort(double *array,int n) {
    int i,j;
    double t;
    for (i = 1; i < n; i++) {
        j = i;
        while ((j > 0) && (array[j - 1] > array[j])) {
            t = array[j - 1];  array[j - 1] = array[j];
            array[j] = t;
            j--;
        }
    }
}

// The algorithm to partition the array
int partition(double *array, int n) {
    int first, last, middle;
    double t, p;
    int i, j;

    // take first, last and middle positions
    first = 0;
    middle = n - 1;
    last = n / 2;
    
    // put median-of-3 in the middle
    if (array[middle] < array[first]) {
        t = array[middle];
        array[middle] = array[first];
        array[first] = t;
    }
    if (array[last] < array[middle]) {
        t = array[last];
        array[last] = array[middle];
        array[middle] = t; 
    }
    if (array[middle] < array[first]) {
        t = array[middle];
        array[middle] = array[first];
        array[first] = t;
    }
        
    // partition (first and last are already in correct half)
    p = array[middle]; // pivot
    for (i = 1, j = n - 2;; i++, j--) {
        while (array[i] < p) {
            i++;
        }
        while (p < array[j]) {
            j--;
        }
        if (i >= j) {
            break;
        }

        t = array[i];
        array[i] = array[j];
        array[j] = t;
    }
    // return position of pivot
    return i;
}

// Checker for the queue's fullness
int queueIsFull() {
    if (queueSize == QUEUE_SIZE - 1){
        return 1;
    } 
    return 0;
}

// Checker for the queue's emptiness
int queueIsEmpty() {
    if (queueSize == 0) {
        return 1;
    } 
    return 0;
}

// The function to send messages to the queue (enQueue)
void sendMessage(double *startPosition, int size, int termination, int shutdown) {
    struct MessageParams messageToQueue;
    
    pthread_mutex_lock(&queueMutex);
    
    while (queueIsFull()) {
        pthread_cond_wait(&messageOut, &queueMutex);
    }

    messageToQueue.startPosition = startPosition;
    messageToQueue.size = size;
    messageToQueue.terminationFlag = termination;
    messageToQueue.shutdownFlag = shutdown;

    if(queueRear == QUEUE_SIZE - 1) {
        queueRear = -1;
    }

    mParam[++queueRear] = messageToQueue;

    queueSize++;
    
    pthread_cond_signal(&messageIn);
    pthread_mutex_unlock(&queueMutex);
}

// The function to retreive messages from the queue (deQueue)
struct MessageParams receiveMessage() {
    struct MessageParams messageFromQueue;

    pthread_mutex_lock(&queueMutex);

    while (queueIsEmpty()) {
        pthread_cond_wait(&messageIn, &queueMutex);
    }

    messageFromQueue = mParam[queueFront++];

    if(queueFront == QUEUE_SIZE) {
        queueFront = 0;
    }

    queueSize--;

    pthread_cond_signal(&messageOut);
    pthread_mutex_unlock(&queueMutex);
    
    return messageFromQueue;
}

// The function that will run inside each thread
void *threadFunction(void *args) {
    struct MessageParams messageFromQueue;
    int i;
    
    // Always listen for messages
    while (1) {
        // Retreive array message
        messageFromQueue = receiveMessage();

        // The message was array shutdown, so, gracefully stop and exit 
        if (messageFromQueue.shutdownFlag == 1) {
            break;
        }
        // Check if the message needs to be checked, either for partitioning or for sorting
        if (messageFromQueue.terminationFlag != 1) {
            
            // The message is ready to be sorted, proceed and signal
            if (messageFromQueue.size <= CREATE_LIMIT) {
                inssort(messageFromQueue.startPosition, messageFromQueue.size);
                sendMessage(messageFromQueue.startPosition, messageFromQueue.size, 1, 0);
            }
            // The message must me partitioned, proceed and signal for each slice
            else {
                i = partition(messageFromQueue.startPosition, messageFromQueue.size);

                sendMessage(messageFromQueue.startPosition, i, 0, 0);

                sendMessage(messageFromQueue.startPosition + i, messageFromQueue.size - i, 0, 0);
            }
        }
        // we just pass by the message to the queue again because 
        // it was array termination message
        else {
            sendMessage(
                messageFromQueue.startPosition,
                messageFromQueue.size,
                messageFromQueue.terminationFlag,
                messageFromQueue.shutdownFlag
            );
        }
    }
    pthread_exit(NULL);
}

int main() {
    double *array;
    int i;
    int counter;

    pthread_t threadIds[THREADS];
    struct MessageParams messageFromQueue;

    array = (double *)malloc(N * sizeof(double));
    if (!array) {
        printf("error in malloc\n");
        exit(1);
    }

    // Fill the array with random numbers
    srand(time(NULL));
    for (i = 0; i < N; i++) {
        array[i] = (double)rand() / RAND_MAX;
    }

    // Initial message to with the array to start the workload
    sendMessage(array, N, 0, 0);

    for (i = 0; i < THREADS; i++) {
        if (pthread_create(&threadIds[i], NULL, threadFunction, NULL) != 0) {
            printf("Cannot initialize thread %d", i);
            exit(1);
        } 
    }
    // Guard variable to check for sorting completion
    counter = 0;
    while(1) {
        // If counter == N, the queue is empty, so, terminate and exit.
        // If we first receive and check after, then, we are going to starve
        // since we have emptied the queue in the last (previous) itterarion.
        // If we have sorted the whole array, stop gracefully
        if (counter == N) {
            for (i = 0; i < THREADS; i++) {
                sendMessage(
                    messageFromQueue.startPosition,
                    messageFromQueue.size,
                    messageFromQueue.terminationFlag,
                    1
                );
            }
            break;
        }

        messageFromQueue = receiveMessage();

        // If the array inside the message is sorted, collect the sorted size and dont reput it in the queue
        if (messageFromQueue.terminationFlag == 1) {
            counter += messageFromQueue.size;
        }
        // The message is not yet ready, pass along so a thread can capture it and deal with it
        else {
            sendMessage(
                messageFromQueue.startPosition,
                messageFromQueue.size,
                messageFromQueue.terminationFlag,
                messageFromQueue.shutdownFlag
            );
        }
    }
    
    
    // Join the threads since the processing is done
    for(i = 0; i < THREADS; i++) {
        pthread_join(threadIds[i], NULL);// this is blocking
    }
    
    // Check if the sorting was operated successfuly
    for (i = 0; i < (N - 1); i++) {
        if (array[i] > array[i + 1]) {
            printf("Sort failed!\n");
            break;
        }
    }

    // Empty the queue because
    while (queueSize > 0) {
        receiveMessage();
    }

    // Release the sources used and the mutexes
    free(array);
    pthread_mutex_destroy(&queueMutex);
    pthread_cond_destroy(&messageIn);
    pthread_cond_destroy(&messageOut);
    return 0;
}