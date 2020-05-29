// code base for various sorting algorithms
// compile with e.g. gcc -O2 -Wall -pthread quicksort.c -o quicksort -DTHREADS=4
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

#include <pthread.h>

#define N 10000000 // 10000
#define CREATE_LIMIT 100
#define QUEUE_SIZE N

// conditional variable, signals a put operation (receiver waits on this)
pthread_cond_t messageIn = PTHREAD_COND_INITIALIZER;
// conditional variable, signals a get operation (sender waits on this)
pthread_cond_t messageOut = PTHREAD_COND_INITIALIZER;

// mutex protecting common resources
pthread_mutex_t queueMutex = PTHREAD_MUTEX_INITIALIZER;
struct ThreadParams {
    int threadIdentifier;
    double *arrayToBeSorted;
};

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
struct MessageParams mParam[QUEUE_SIZE]; // the queue of messages

void inssort(double *a,int n) {
    int i,j;
    double t;
    for (i = 1; i < n; i++) {
        j = i;
        while ((j > 0) && (a[j - 1] > a[j])) {
            t = a[j - 1];  a[j - 1] = a[j];
            a[j] = t;
            j--;
        }
    }
}

int partition(double *a, int n) {
    int first, last, middle;
    double t, p;
    int i, j;

    // take first, last and middle positions
    first = 0;
    middle = n - 1;
    last = n / 2;
    
    // put median-of-3 in the middle
    if (a[middle] < a[first]) {
        t = a[middle];
        a[middle] = a[first];
        a[first] = t;
    }
    if (a[last] < a[middle]) {
        t = a[last];
        a[last] = a[middle];
        a[middle] = t; 
    }
    if (a[middle] < a[first]) {
        t = a[middle];
        a[middle] = a[first];
        a[first] = t;
    }
        
    // partition (first and last are already in correct half)
    p = a[middle]; // pivot
    for (i = 1, j = n - 2;; i++, j--) {
        while (a[i] < p) {
            i++;
        }
        while (p < a[j]) {
            j--;
        }
        if (i >= j) {
            break;
        }

        t = a[i];
        a[i] = a[j];
        a[j] = t;
    }
    // return position of pivot
    return i;
}

int queueIsFull() {
    if (queueSize == QUEUE_SIZE - 1){
        return 1;
    } 
    return 0;
}

int queueIsEmpty() {
    if (queueSize == 0) {
        return 1;
    } 
    return 0;
}

void sendMessage(double *startPosition, int size, int termination, int shutdown) {
    struct MessageParams messageToQueue;
    
    pthread_mutex_lock(&queueMutex);
    
    while (queueIsFull()) {
        // printf("\nWasting cycles in sendMessage(), queue is full, wait for something to get dequeued\n");
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

struct MessageParams receiveMessage() {
    struct MessageParams messageFromQueue;

    pthread_mutex_lock(&queueMutex);

    while (queueIsEmpty()) {
        // printf("\nWasting cycles in receiveMessage(), queue is empty\n");
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

void *threadFunction(void *args) {
    struct ThreadParams *tParam = (struct ThreadParams *)args; 
    int id = tParam->threadIdentifier;
    
    struct MessageParams messageFromQueue;
    int i;

    while (1) {
        messageFromQueue = receiveMessage();

        if (messageFromQueue.shutdownFlag == 1) {
            break;
        }
        if (messageFromQueue.terminationFlag != 1) {
            if (messageFromQueue.size <= CREATE_LIMIT) {
                inssort(messageFromQueue.startPosition, messageFromQueue.size);
                sendMessage(messageFromQueue.startPosition, messageFromQueue.size, 1, 0);
            }
            else {
                i = partition(messageFromQueue.startPosition, messageFromQueue.size);

                sendMessage(messageFromQueue.startPosition, i, 0, 0);

                sendMessage(messageFromQueue.startPosition + i, messageFromQueue.size - i, 0, 0);
            }
        }
        else {
            // we just pass by the message to the queue again because 
            // it was a termination message
            sendMessage(
                messageFromQueue.startPosition,
                messageFromQueue.size,
                messageFromQueue.terminationFlag,
                messageFromQueue.shutdownFlag
            );
        }
    }
    printf("Thread: %d, just terminated\n", id);
    pthread_exit(NULL);
}

int main() {
    double *a;
    int i;
    int counter;

    pthread_t threadIds[THREADS];
    struct ThreadParams tParam[THREADS];
    struct MessageParams messageFromQueue;

    a = (double *)malloc(N * sizeof(double));
    if (!a) {
        printf("error in malloc\n");
        exit(1);
    }

    // fill array with random numbers
    srand(time(NULL));
    for (i = 0; i < N; i++) {
        a[i] = (double)rand() / RAND_MAX;
    }

    sendMessage(a, N, 0, 0);

    for (i = 0; i < THREADS; i++) {
        tParam[i].threadIdentifier = i;
        tParam[i].arrayToBeSorted = a;
        if (pthread_create(&threadIds[i], NULL, threadFunction, &tParam[i]) != 0) {
            printf("Cannot initialize thread %d", i);
            exit(1);
        } 
    }

    counter = 0;
    while(1) {
        // if counter == N the queue is empty, so, terminate and exit
        // if we first receive and check after, that we are going to starve,
        // since we have emptied the queue in the last itterarion
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

        if (messageFromQueue.terminationFlag == 1) {
            counter += messageFromQueue.size;
        }
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
        if (a[i] > a[i + 1]) {
            printf("Sort failed!\n");
            break;
        }
    }

    //free the array
    while (queueSize > 0) {
        receiveMessage();
    }

    free(a);    
    pthread_mutex_destroy(&queueMutex);
    pthread_cond_destroy(&messageIn);
    pthread_cond_destroy(&messageOut);
    return 0;
}