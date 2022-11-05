#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <pthread.h>
#include <vector>
#include <unordered_set>
#include <unordered_map>

using namespace std;

typedef struct input_data {
    int *arr;
    int n;
} input_data;

void parse_cmd_args(char *argv[], int *mapper_count, int *reduce_count, char *input_filename) {
    *mapper_count = atoi(argv[1]);
    *reduce_count = atoi(argv[2]);
    strcpy(input_filename, argv[3]);
}

typedef struct thread_arg {
    int id, mapper_count, reduce_count, isMapper;
    int* files_left;
    input_data* data;
    pthread_mutex_t* mutex;
    pthread_barrier_t* barrier;
    vector<vector<vector<int>>>* buckets;
    vector<vector<unordered_map<int, int>>>* cache;
} thread_arg;

input_data* parse_input_file(char *input_filename) {
    FILE *input_file = fopen(input_filename, "r");

    int number_of_input_files;
    fscanf(input_file, "%d", &number_of_input_files);

    input_data* input_data_arr = (input_data*) malloc(number_of_input_files * sizeof(input_data));

    for (int i = 0; i < number_of_input_files; i++) {
        char input_data_filename[200];
        fscanf(input_file, "%s", input_data_filename);

        FILE *input_data_file = fopen(input_data_filename, "r");
        int numbers_count;
        fscanf(input_data_file, "%d", &numbers_count);

        input_data_arr[i].arr = (int *) malloc(numbers_count * sizeof(int));
        input_data_arr[i].n = numbers_count;
        
        for (int j = 0; j < numbers_count; j++) {
            fscanf(input_data_file, "%d", &input_data_arr[i].arr[j]);
        }

        fclose(input_data_file);
    }

    fclose(input_file);

    return input_data_arr;
}

int get_number_of_input_files(char *input_filename) {
    FILE *input_file = fopen(input_filename, "r");
    int number_of_input_files;

    fscanf(input_file, "%d", &number_of_input_files);

    fclose(input_file);

    return number_of_input_files;
}

void free_input_data(input_data *input_data_arr, int input_data_arr_count) {
    for (int i = 0; i < input_data_arr_count; i++) {
        free(input_data_arr[i].arr);
    }
    free(input_data_arr);
}

int isPerfectPower(int number, int exponent) {
    unsigned long long left, right;
    left = 1;
    right = number;

    while (left <= right) {
        unsigned long long m = left + (right - left) / 2;
        unsigned long long maybe_number = m;

        for (int i = 0; i < exponent - 1; i++) {
            maybe_number *= m;

            if (maybe_number > number) {
                break;
            }
        }

        if (maybe_number > number) {
            right = m - 1;
        } else if (maybe_number < number) {
            left = m + 1;
        } else {
            return 1;
        }
    }

    return 0;
}

void* thread_function(void *arg) {
    thread_arg args = *(thread_arg*) arg;
    
    if (args.isMapper) {
        int current_file;

        while(*(args.files_left) > 0) {
            pthread_mutex_lock(args.mutex);

            if (*(args.files_left) > 0) {
                current_file = *(args.files_left) - 1;
                *(args.files_left) = *(args.files_left) - 1;
            } else {
                current_file = -1;
            }

            pthread_mutex_unlock(args.mutex);

            if (current_file == -1) {
                break;
            }

            for (int i = 0; i < args.reduce_count; i++) {
                int exponent = i + 2;

                input_data current_data = args.data[current_file];
                for (int j = 0; j < current_data.n; j++) {
                    if ((*(args.cache))[args.id][i].find(current_data.arr[j]) != (*(args.cache))[args.id][i].end()) {
                        if ((*(args.cache))[args.id][i][current_data.arr[j]] == 1) {
                            (*(args.buckets))[args.id][i].push_back(current_data.arr[j]);
                        }
                    } else {
                        (*(args.cache))[args.id][i][current_data.arr[j]] = isPerfectPower(current_data.arr[j], exponent);
                        
                        if (current_data.arr[j] == 1) {
                            (*(args.cache))[args.id][i][current_data.arr[j]] = 1;
                        }

                        if ((*(args.cache))[args.id][i][current_data.arr[j]] == 1) {
                            (*(args.buckets))[args.id][i].push_back(current_data.arr[j]);
                        }
                    }
                }
            }
        }
    }

    pthread_barrier_wait(args.barrier);

    if (!args.isMapper) {
        int exponent = args.id + 2;
        unordered_set<int> set;

        for (int i = 0; i < args.mapper_count; i++) {
            for (int j = 0; j < (*(args.buckets))[i][args.id].size(); j++) {
                set.insert((*(args.buckets))[i][args.id][j]);
            }
        }

        char output_filename[20] = "out";
        sprintf(output_filename, "out%d.txt", exponent);

        FILE* output_file = fopen(output_filename, "w");

        fprintf(output_file, "%d", set.size());

        fclose(output_file);
    }

    return NULL;
}

int main(int argc, char *argv[]) {
    int mapper_count, reduce_count;
    char input_filename[200];

    parse_cmd_args(argv, &mapper_count, &reduce_count, input_filename);

    input_data *input_data_arr = parse_input_file(input_filename);
    int input_data_arr_count = get_number_of_input_files(input_filename);
    int files_left = input_data_arr_count;

    pthread_t mapper_threads[mapper_count];
    pthread_t reduce_threads[reduce_count];
    
    thread_arg mapper_args[mapper_count];
    thread_arg reducer_args[reduce_count];

    pthread_mutex_t mutex;
    pthread_barrier_t barrier;

    pthread_mutex_init(&mutex, NULL);
    pthread_barrier_init(&barrier, NULL, mapper_count + reduce_count);

    vector<vector<vector<int>>> mapper_buckets(mapper_count, vector<vector<int>>(reduce_count, vector<int>()));
    vector<vector<unordered_map<int, int>>> mapper_cache(mapper_count, vector<unordered_map<int, int>>(reduce_count, unordered_map<int, int>()));

    int return_code;
    for (int i = 0; i < mapper_count + reduce_count; i++) {
        if (i < mapper_count) {
            mapper_args[i].id = i;
            mapper_args[i].isMapper = 1;
            mapper_args[i].files_left = &files_left;
            mapper_args[i].data = input_data_arr;
            mapper_args[i].mapper_count = mapper_count;
            mapper_args[i].reduce_count = reduce_count;
            mapper_args[i].mutex = &mutex;
            mapper_args[i].barrier = &barrier;
            mapper_args[i].buckets = &mapper_buckets;
            mapper_args[i].cache = &mapper_cache;
            pthread_create(&mapper_threads[i], NULL, thread_function, (void *) &mapper_args[i]);
        } else {
            reducer_args[i - mapper_count].id = i - mapper_count;
            reducer_args[i - mapper_count].isMapper = 0;
            reducer_args[i - mapper_count].mapper_count = mapper_count;
            reducer_args[i - mapper_count].reduce_count = reduce_count;
            reducer_args[i - mapper_count].mutex = &mutex;
            reducer_args[i - mapper_count].barrier = &barrier;
            reducer_args[i - mapper_count].buckets = &mapper_buckets;
            pthread_create(&reduce_threads[i - mapper_count], NULL, thread_function, (void *) &reducer_args[i - mapper_count]);
        }
    }

    for (int i = 0; i < mapper_count + reduce_count; i++) {
        if (i < mapper_count) {
            pthread_join(mapper_threads[i], NULL);
        } else {
            pthread_join(reduce_threads[i - mapper_count], NULL);
        }
    }

    pthread_mutex_destroy(&mutex);
    pthread_barrier_destroy(&barrier);

    free_input_data(input_data_arr, input_data_arr_count);
}