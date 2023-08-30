import os
import shutil
from mpi4py import MPI
from WorkerProcess import WorkerProcess


def getFilesFromDirectory(dir):
    f = []
    for root, dirs, files in os.walk(dir):
        for file in files:
            f.append(os.path.join(root, file))
    return f


def get_file_name(file_path):
    return file_path.rsplit("\\", 1)[-1]


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
status = MPI.Status()
print('My rank is ', rank)
if rank == 0:
    # stergem fisierul rezultat
    os.remove("rezultat.txt")

    # preluam lista de fisiere
    directory = "files"
    file_list = os.listdir(directory)
    no_files = len(file_list)

    # refacem directorul intermediar
    shutil.rmtree("D:\\School\\APD Project\\intermediar", ignore_errors=True)
    os.mkdir("intermediar")

    # trimitem fiecarui proces fisierele de care se ocupa
    for n in range(no_files):
        # Avem size-1 procese disponibile pentru procesare, iar primul este p1, de aici n % (size-1) + 1
        comm.send(file_list[n], dest=n % (size - 1) + 1)

    # trimitem fiecarui proces mesajul de incepere a maparii
    for n in range(1, size):
        comm.send("Poti incepe maparea", dest=n, tag=1)

    # primim de la procese semnul ca au terminat etapa de mapare
    gata_mapare = 0
    for n in range(1, size):
        comm.recv(source=n, status=status)
        if status.Get_tag() == 2:
            gata_mapare = gata_mapare + 1
    if gata_mapare == size - 1:
        print("Toate procesele worker au terminat maparea.")
        intermediary_files = getFilesFromDirectory("D:\\School\\APD Project\\intermediar")
        # sortam lista alfabetic
        sorted_files = sorted(intermediary_files, key=get_file_name)
        # formam dictionarul de frecvente
        file_count = {}
        for file in sorted_files:
            file_name = file.split("\\")[-1]
            if file_name in file_count:
                file_count[file_name] += 1
            else:
                file_count[file_name] = 1
        # trimitem fiecarui proces cate un nume de fisier si caile catre acesta
        # daca ana.txt exista in doua directoare intermediare primul proces va primi doua cai(doua fisiere de procesat)
        iterator = iter(file_count.items())
        index = 0
        for i in range(len(file_count)):
            file_pair = next(iterator)
            for j in range(file_pair[1]):
                comm.send(sorted_files[index], dest=i % (size - 1) + 1)
                index = index + 1

        # refacem forderul rezultat
        shutil.rmtree("D:\\School\\APD Project\\rezultat", ignore_errors=True)
        os.mkdir("rezultat")

        # trimitem fiecarui proces mesajul de incepere a procesarii
        for n in range(1, size):
            comm.send("Poti incepe reducerea", dest=n, tag=3)

        # asteptam sa termine toata lumea reducerea
        gata_reducere = 0
        for n in range(1, size):
            comm.recv(source=n, status=status)
            if status.Get_tag() == 4:
                gata_reducere += 1
        if gata_reducere == size - 1:
            print("Toate procesele worker au terminat reducerea.")
            # preluam fisierele rezultat
            result_files = os.listdir("rezultat")
            result = open("rezultat.txt", "a", errors="ignore")
            for r_file in result_files:
                name = r_file.split(".txt")[0]
                contentf = open("rezultat/" + r_file, "r")
                content = contentf.read()
                result.write(name + ":" + content + "\n")
            result.close()


else:
    workerProcess = WorkerProcess(rank)
    #primim fisierele de mapat
    while True:
        file_name = comm.recv(source=0, status=status)
        if status.Get_tag() == 1:
            break
        workerProcess.addFile(file_name)
    #mapam fisierele
    workerProcess.mapFiles()
    # anuntam terminarea fazei de mapare
    comm.send("Am terminat maparea", dest=0, tag=2)
    # primim fisierele de redus
    while True:
        file_path = comm.recv(source=0, status=status)
        if status.Get_tag() == 3:
            break
        workerProcess.addFile(file_path)
    # reducem fisierele
    workerProcess.reduceFiles()
    # anuntam terminarea fazei de reducere
    comm.send("Am terminat reducerea", dest=0, tag=4)
