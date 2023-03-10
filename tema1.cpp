#include <iostream>
#include <pthread.h>
#include <math.h>
#include <unistd.h>
#include <fstream>
#include <stack>
#include <list>
#include <map>
#include <bits/stdc++.h>

using namespace std;

struct threadArgs {
    // id-ul threadului
    int id;
    // numarul de threaduri mappers
    int mappers;
    // numarul de threaduri reducers
    int reducers;
    // numarul exponentului de care se ocupa threadul Reducer
    int exp;
    // numarul exponentului maxim
    int e;
    // mapare intre exponent si lista de puteri
    map <int, map<int,list<int>>> *allLists;
    // mapare intre exponent si setul de puteri
    map <int, unordered_set<int>> *finalLists;
    // mapare intre exponent si toate puterile posibile
    map <int, set<int>> *pows;
    // fisierele care trebuie impartite mapperilor
    priority_queue <pair <int, string>> *filesToBeShared;
    pthread_barrier_t *barrier;
    pthread_mutex_t *mutex1, *mutex2;
};
void *f(void *arg) {
    struct threadArgs args= *(struct threadArgs *) arg;
    map <int, list<int>> partial_lists;
    list <int> List;
    for (int j = 2; j <= args.e; j++) {
        partial_lists.insert(pair <int, list<int>>(j, List));
    }
    int thread_id = args.id;

    // daca este thread de tip Map
    if (thread_id < args.mappers) {
        ifstream stream;
        int n, value;
        string s, current_file;

        // extragem cate un fisier pentru a fi prelucrat
        while (true) {
            pthread_mutex_lock(args.mutex1);
            if (!args.filesToBeShared->empty()) {
                current_file = args.filesToBeShared->top().second;
            }
            else {
                pthread_mutex_unlock(args.mutex1);
                break;
            }
            args.filesToBeShared->pop();
            pthread_mutex_unlock(args.mutex1);
            stream.open(current_file);
            // extrag cate un numar din fisier
            if (stream.is_open()) {
                stream >> s;
                n = stoi(s);
                for (int i = 0; i < n; i++) {
                    stream >> s;
                    value = stoi(s);
                    if (value > 0) {
                        // verificam daca numarul se afla in set-ul de puteri
                        for (int exp = 2; exp <= args.e; exp++) {
                            bool is_power = false;
                            auto it = args.pows->find(exp);
                            auto itr = it->second.find(value);
                            if (itr != it->second.end()) {
                                is_power = true;
                            }
                            // numarul e putere => il punem in lista threadului
                            if (is_power ==  true) {
                                auto iterator = partial_lists.find(exp);
                                iterator->second.push_back(value);
                            }
                        }
                    }
                }
                stream.close();
            }
        }
        // inseram listele de puteri ale threadului in mapul cu toate listele
        pthread_mutex_lock(args.mutex2);
        args.allLists->insert
        (pair <int, map <int, list <int>>>(thread_id, partial_lists));
        pthread_mutex_unlock(args.mutex2);
    }
    // asteptam toate threadurile sa ajunga la bariera
    pthread_barrier_wait(args.barrier);

    // thread de tip Reducer
    if (thread_id >= args.mappers) {
        unordered_set <int> setOfPows;
        // inseram fiecare element din mapul cu toate listele intr-un set
        // cu puteri unice
        for (auto itr = args.allLists->begin(); 
            itr != args.allLists->end(); itr++) {
            auto it = itr->second.find(args.exp);
            for(auto elem : it->second) {
                setOfPows.insert(elem);
            }
        }
        // adaugam set-ul la mapul cu set-uri finale asociate fiecare exponent
        pthread_mutex_lock(args.mutex2);
        args.finalLists->insert
        (pair<int, unordered_set<int>>(args.exp, setOfPows));
        pthread_mutex_unlock(args.mutex2);
    }
  	pthread_exit(NULL);
}

int main(int argc, char **argv){
    char *file_input = argv[3];
    int reducers = atoi(argv[2]);
    int mappers = atoi(argv[1]);
    int nrThreads, i;
    int e = atoi(argv[2]) + 1;
    nrThreads = mappers + reducers;
    pthread_t threads [mappers + reducers + 1];
    pthread_barrier_t barrier;
    pthread_mutex_t mutex1, mutex2;
    pthread_mutex_init(&mutex1, NULL);
    pthread_mutex_init(&mutex2, NULL);
    pthread_barrier_init(&barrier, NULL, mappers + reducers);
    string s;
    int N, r;
	void *status;
    priority_queue <pair <int, string>> filesToBeShared;
    struct threadArgs args[mappers + reducers + 1];
    map <int, map <int, list <int>>> allLists;
    map <int, unordered_set<int>> finalLists;
    map <int, set<int>> pows;;

    // citim din fisierul de input
    ifstream file;
    file.open(file_input);
    if (file.is_open() ) {
        file >> s;
        N = stoi(s);
        // punem fisierele intr-o coada de prioritati in functie de dimensiune
        for (int i = 0; i < N; i++){
            file >> s;
            ifstream in_file(s, ios::binary);
            in_file.seekg(0, ios::end);
            int file_size = in_file.tellg();
            filesToBeShared.push(make_pair(file_size, s));
        }
    }
    // calculam toate puterile pentru toti exponentii posibili
    for (int i = 2; i <= e; i++) {
        set<int> expSet;
        for (int j = 1; pow(j, i) < INT_MAX; j++) {
            expSet.insert(pow(j, i));
        }
        pows.insert(pair<int, set<int>>(i, expSet));
    }
    // initializam structura fiecarui thread Mapper
    for (i = 0; i < mappers; i++) {
        args[i].e = e;
        args[i].filesToBeShared = &filesToBeShared;
        args[i].mutex1 = &mutex1;
        args[i].mutex2 = &mutex2;
        args[i].barrier = &barrier;
        args[i].id = i;
        args[i].mappers = mappers;
        args[i].reducers = reducers;
        args[i].allLists = &allLists;
        args[i].finalLists = &finalLists;
        args[i].pows = &pows;
    }
    int exp = 2;
    // initializam structura fiecarui thread Reducer
    for (int k = mappers ; k < nrThreads; k++) {
        args[k].e = e;
        args[k].exp = exp;
        args[k].filesToBeShared = &filesToBeShared;
        args[k].mutex1 = &mutex1;
        args[k].mutex2 = &mutex2;
        args[k].barrier = &barrier;
        args[k].id = k;
        args[k].mappers = mappers;
        args[k].reducers = reducers;
        args[k].allLists = &allLists;
        args[k].finalLists = &finalLists;
        args[k].pows = &pows;
        exp++;
    }
    // cream threadurile
    for (int i = 0; i < nrThreads; i++) {
        r = pthread_create(&threads[i], NULL, f, &args[i]);
		if (r) {
			printf("Eroare la crearea thread-ului %d\n", i);
			exit(-1);
		}
    }
    // threadurile fac join pentru a prelua rezultatul final
    for (int i = 0; i < nrThreads; i++) {
		r = pthread_join(threads[i], &status);
		if (r) {
			printf("Eroare la asteptarea thread-ului %d\n", i);
			exit(-1);
		}
	}
    // cream fisierele out unde scriem numarul de puteri gasite
    auto it = finalLists.begin();
    ofstream output;
    for (int i = 2; i <= e; i++ && it != finalLists.end()) {
        string s = "out" + to_string(it->first) + ".txt";
        output.open(s);
        output << it->second.size();
        it++;
        output.close();
    }
    pthread_mutex_destroy(&mutex1);
    pthread_mutex_destroy(&mutex2);
    pthread_barrier_destroy(&barrier);
    return 0;
}