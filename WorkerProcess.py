import os
import re
from pathlib import Path
reserved_words = ["con", "prn", "aux", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8"]

def process_text(t):
    t = re.sub(r'[^\w\s]', ' ', t)
    t = t.replace("\n", " ")
    t = t.replace("\t", " ")
    t = re.sub(' +', ' ', t)
    return t


class WorkerProcess:
    def __init__(self, rank):
        self.files = []
        self.rank = rank

    def addFile(self, file_name):
        self.files.append(file_name)

    def getFiles(self):
        return self.files

    def createDirectory(self):
        try:
            os.mkdir("intermediar\\" + str(self.rank))
        except Exception:
            print("Directory already exists.")

    def mapFiles(self):
        #creem directorul cu numele rank-ului norstu in directorul "intermediar"
        self.createDirectory()
        for index in range(len(self.files)):
            # citim si procesam continutul fisierului
            file_name = self.files[index]
            file = open("files/" + file_name, "r", encoding="unicode-escape")
            file_content = file.read()
            file_content = process_text(file_content)
            file_content = file_content.split(" ")
            file_content = list(map(lambda w: w.lower(), file_content))
            #scriem rezultatele in fisierele intermediare
            for word in file_content:
                if word not in reserved_words and word:
                    with open("intermediar/" + str(self.rank) + "/" + word + ".txt", "a", errors="ignore") as intermediar_result_file:
                            intermediar_result_file.write(file_name + " ")
            file.close()
        self.files = []

    def reduceFiles(self):
        # functie lambda pentru a forma lista doar cu nume(fara calea fisierului)
        getName = lambda string: string.rsplit("\\", 1)[-1]
        file_names = list(map(getName, self.files))

        # pentru fieacare fisier formam dictionarul cu frecvente si le scriem in fisierul corespunzator din folderul rezultat
        for index in range(len(self.files)):
            file = self.files[index]
            fisier = open(file, "r")
            file_content = fisier.read()
            file_content = file_content.split(" ")
            freq_dict = {}
            for f in file_content:
                if f:
                    if f in freq_dict:
                        freq_dict[f] = freq_dict[f] + 1
                    else:
                        freq_dict[f] = 1
            result = open("rezultat/" + file_names[index], "a")
            res_string = ""
            for item in freq_dict:
                res_string += "(" + item + "," + str(freq_dict[item]) + ")"
            result.write(res_string)
            fisier.close()
            result.close()
        self.files = []
