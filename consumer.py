import json
import threading
from kafka import KafkaConsumer
import re
import time
file3 = open("message sequence consumer.txt","w")
file3.close()
def thread_function(i):
    count = 1
    if fid[i] == "194161014-4148" or fid[i] == "194161014-4158" :
       file1 = open(fid[i]+ "_scorecardsequence.txt", "w")

    for message in consumers[i]:
        file = open(fid[i] + "_com(kafka).txt", "a+")
        if (fid[i] == "194161014-4148" or fid[i] == "194161014-4158" ) and count > 10:
            scorecards(fid[i])
        file.write(message.value.decode())
        file.close()
        count = count+1
        file3 = open("message sequence consumer.txt","a+")
        file3.write(fid[i]+"     "+message.value.decode())
        file3.close()

def scorecards(inp):

    file1 = open(inp + "_scorecardsequence.txt", "a+")
    file2 = open(inp +"_finalscorecard.txt","w")
    file1.write("IST INNING MATCH " + inp + "\n")
    file2.write("IST INNING MATCH " + inp + "\n")

    def getsquad():
        f = open(m)
        for l in f:
            if "#" not in l:

                a = l.split(",")
            elif 1:
                break
        for l in f:
            if "#" not in l:

                b = l.split(",")
            elif 1:
                break
                f.close()

        return a, b

    def fow():

        f = open(m)
        r = 0
        b = 0
        mi = 0
        nf = 0
        ns = 0
        sr = 0
        tb = 0
        extra = 0
        nb = 0
        wi = 0
        lb = 0
        ck = 0
        s = ""
        b4 = 0

        for l in f:

            if "##########" not in l:
                bi = re.search('[a-z]*[A-Z]*[a-z]+ to [A-z]*[a-z]+', l)
                if bi != None:

                    x, y = bi.group().split(" to ")

                    e = re.sub(r"\s+", "", l).split(",")

                    if "1run" in e:
                        r = r + 1
                        b = b + 1

                    elif "2runs" in e:
                        r = r + 2
                        b = b + 1
                    elif "3runs" in e:
                        r = r + 3
                        b = b + 1
                    elif "FOURruns" in e or "4runs" in e:
                        r = r + 4
                        b = b + 1
                        nf = nf + 1
                    elif "SIXruns" in e or "6runs" in e:
                        r = r + 6
                        b = b + 1
                        ns = ns + 1
                    elif "norun" in e:
                        b = b + 1
                    elif "1wide" in e:
                        extra = extra + 1
                        wi = wi + 1
                    elif "2wides" in e:
                        extra = extra + 2
                        wi = wi + 2
                    elif "3wides" in e:
                        extra = extra + 3
                        wi = wi + 3
                    elif "4wides" in e:
                        extra = extra + 4
                        wi = wi + 4
                    elif "5wides" in e:
                        extra = extra + 5
                        wi = wi + 5
                    elif "(noball)FOURruns" in e:
                        extra = extra + 1
                        r = r + 4
                        nf = nf + 1
                        b = b
                        nb = nb + 1
                        tb = tb - 1
                    elif "(noball)" in e or "1noball" in e:
                        extra = extra + 1

                        tb = tb - 1
                        nb = nb
                    elif "(noball)1run" in e:
                        extra = extra + 1
                        r = r + 1
                        tb = tb - 1

                        nb = nb
                    elif "(noball)2runs" in e:
                        extra = extra + 1
                        r = r + 2
                        tb = tb - 1
                        b = b
                    elif "(noball)3runs" in e:
                        extra = extra + 1
                        r = r + 3
                        tb = tb - 1
                        b = b
                        nb = nb + 1
                    elif "(noball)SIXruns" in e:
                        extra = extra + 1
                        r = r + 6
                        tb = tb - 1
                        b = b
                        ns = ns + 1



                    elif "1legbye" in e or "1bye" in e:
                        if "1bye" in e:
                            b4 = b4 + 1
                        else:
                            lb = lb + 1
                        extra = extra + 1
                        b = b + 1
                    elif "2legbyes" in e or "2byes" in e:
                        if "2byes" in e:
                            b4 = b4 + 2
                        else:
                            lb = lb + 2
                        extra = extra + 2
                        b = b + 1
                    elif "3legbyes" in e or "3byes" in e:
                        if "3byes" in e:
                            b4 = b4 + 2
                        else:
                            lb = lb + 3
                        extra = extra + 3
                        b = b + 1
                    elif "FOURlegbyes" in e or "4legbyes" in e or "4byes" in e:
                        if "4byes" in e:
                            b4 = b4 + 4
                        else:
                            lb = lb + 4
                        extra = extra + 4
                        b = b + 1
                    elif "5runs" in e:
                        r = r + 5
                        b = b + 1
                    elif "6runs" in e:
                        r = r + 6
                        b = b + 1
                    elif "OUT" in e:
                        b = b + 1
                        ck = ck + 1
                        s = s + str(ck) + "-" + str(r + extra) + "( " + y + " , " + str((b) // 6) + "." + str(
                            (b) % 6) + " overs ),"

                    elif "1runOUT" in e:
                        r = r + 1
                        b = b + 1
                        ck = ck + 1
                        s = s + str(ck) + "-" + str(r + extra) + "( " + y + " , " + str((b) // 6) + "." + str(
                            (b) % 6) + " overs ),"

        return s

    class batting:
        squad1 = []  # 2nd ining bat
        squad2 = []
        nick1 = []
        batting1 = {}
        fow = {}
        pt = 0
        extra = 0
        lb = 0
        nb = 0
        wi = 0
        total = 0
        wk = 0
        tb = 0
        tr = 0

        def nick(self):
            f = open(m)
            k = 0
            for i in range(0, 11):
                self.batting1[self.squad1[i]] = ["", "", "", "", "", "", "", "", self.squad1[i]]

            for l in f:
                if "##########" not in l:

                    b = re.search('[A-Z][a-z]+ to [A-Z]*[a-z]+', l)
                    if b != None:
                        x, y = b.group().split(" to ")

                        for i in range(0, 11):
                            if y in self.squad1[i] and self.batting1[self.squad1[i]][0] == "":

                                for j in range(0, 11):
                                    if x in self.squad2[j]:
                                        self.nick1.append(y)

                                        self.batting1[self.squad1[i]][0] = y
                                        break





                else:
                    f.close()
                    break
            # self.nick1.reverse()

        def batt(self):
            extra = 0
            lb = 0
            nb = 0
            wi = 0
            total = 0
            wk = 0
            tb = 0
            tr = 0
            tr1 = 0
            ttb = 0
            wkk = 0
            b4 = 0

            for i in range(0, 11):
                co = self.batting1[self.squad1[i]][0]
                f = open(m)
                r = 0
                b = 0
                mi = 0
                nf = 0
                ns = 0
                sr = 0

                for l in f:

                    if "##########" not in l:
                        bi = re.search('[a-z]*[A-Z]*[a-z]+ to [A-z]*[a-z]+', l)
                        if bi != None:
                            x, y = bi.group().split(" to ")
                            if y == (co):

                                e = re.sub(r"\s+", "", l).split(",")

                                if "1run" in e:
                                    r = r + 1
                                    b = b + 1

                                elif "2runs" in e:
                                    r = r + 2
                                    b = b + 1
                                elif "3runs" in e:
                                    r = r + 3
                                    b = b + 1
                                elif "FOURruns" in e or "4runs" in e:
                                    r = r + 4
                                    b = b + 1
                                    nf = nf + 1
                                elif "SIXruns" in e or "6runs" in e:
                                    r = r + 6
                                    b = b + 1
                                    ns = ns + 1
                                elif "norun" in e:
                                    b = b + 1
                                elif "1wide" in e:
                                    extra = extra + 1
                                    wi = wi + 1
                                elif "2wides" in e:
                                    extra = extra + 2
                                    wi = wi + 2
                                elif "3wides" in e:
                                    extra = extra + 3
                                    wi = wi + 3
                                elif "4wides" in e:
                                    extra = extra + 4
                                    wi = wi + 4
                                elif "5wides" in e:
                                    extra = extra + 5
                                    wi = wi + 5
                                elif "(noball)FOURruns" in e:
                                    extra = extra + 1
                                    r = r + 4
                                    nf = nf + 1
                                    b = b + 1
                                    nb = nb + 1
                                    tb = tb - 1
                                elif "(noball)" in e or "1noball" in e:
                                    extra = extra + 1
                                    b = b + 1
                                    tb = tb - 1
                                    nb = nb + 1
                                elif "(noball)1run" in e or "2noball" in e:
                                    extra = extra + 1
                                    r = r + 1
                                    tb = tb - 1
                                    b = b + 1
                                    nb = nb + 1
                                elif "(noball)2runs" in e:
                                    extra = extra + 1
                                    r = r + 2
                                    tb = tb - 1
                                    b = b + 1
                                elif "(noball)3runs" in e:
                                    extra = extra + 1
                                    r = r + 3
                                    tb = tb - 1
                                    b = b + 1
                                    nb = nb + 1
                                elif "(noball)SIXruns" in e:
                                    extra = extra + 1
                                    r = r + 6
                                    tb = tb - 1
                                    b = b + 1
                                    ns = ns + 1
                                elif "OUT" in e:
                                    b = b + 1
                                    wk = wk + 1
                                elif "1runOUT" in e:
                                    b = b + 1
                                    r = r + 1
                                    wk = wk + 1




                                elif "1legbye" in e or "1bye" in e:
                                    if "1bye" in e:
                                        b4 = b4 + 1
                                    else:
                                        lb = lb + 1
                                    extra = extra + 1
                                    b = b + 1
                                elif "2legbyes" in e or "2byes" in e:
                                    if "2byes" in e:
                                        b4 = b4 + 2
                                    else:
                                        lb = lb + 2
                                    extra = extra + 2
                                    b = b + 1
                                elif "3legbyes" in e or "3byes" in e:
                                    if "3byes" in e:
                                        b4 = b4 + 3
                                    else:
                                        lb = lb + 3
                                    extra = extra + 3
                                    b = b + 1
                                elif "FOURlegbyes" in e or "4legbyes" in e or "4byes" in e:
                                    if "4byes" in e:
                                        b4 = b4 + 4
                                    else:
                                        lb = lb + 4
                                    extra = extra + 4
                                    b = b + 1
                                elif "5runs" in e:
                                    r = r + 5
                                    b = b + 1
                                elif "6runs" in e:
                                    r = r + 6
                                    b = b + 1

                        bil = re.compile(r'([0-9]*)m ([0-9]*b [0-9]*x4 [0-9]*x6)')
                        b1 = bil.search(l)

                        if b1 != None and co in l and co != "":
                            l = l[::-1]

                            self.batting1[self.squad1[i]][1] = l[(l.index("(") + 5):][::-1]
                            self.batting1[self.squad1[i]][4] = b1.group(1)

                        if b != 0 and self.batting1[self.squad1[i]][1] == "":
                            self.batting1[self.squad1[i]][1] = self.squad1[i].rstrip() + "  not out "






                    else:

                        break
                self.batting1[self.squad1[i]][2] = str(r)
                self.batting1[self.squad1[i]][3] = str(b)
                # self.batting1[self.squad1[i]][4]= mi
                self.batting1[self.squad1[i]][5] = str(nf)
                self.batting1[self.squad1[i]][6] = str(ns)

                if b == 0:
                    self.batting1[self.squad1[i]][7] = str(0)
                else:

                    self.batting1[self.squad1[i]][7] = str(round(r * 100 / b, 2))
                f.close()
                tr = tr + r
                tb = tb + b
                self.tr1 = tr + extra
                self.ttb = tb
                self.wkk = wk
                self.extra = str(extra) + "( b " + str(b4) + " lb " + str(lb) + " w " + str(wi) + " nb " + str(nb) + ")"
                self.tr = str(tr + extra) + "/" + str(wk) + "( " + str(tb // 6) + "." + str(tb % 6) + " overs )"

        # print(tr+extra, str(tb//6)+"."+str(tb%6), extra , nb , wi, lb, wk)

        def batcard(self):
            s = "\nDID NOT BAT :"
            batt = {}

            print("batsman".ljust(40), "R".center(8), "B".center(8), "M".center(8), "4s".center(8), "6s".center(8),
                  "SR".center(8))

            file1.write(
                "\n\nbatsman".ljust(50) + "R".center(8) + "B".center(8) + "M".center(8) + "4s".center(8) + "6s".center(
                    8) + "SR".center(8) + "\n\n")
            file2.write(
                "\n\nbatsman".ljust(50) + "R".center(8) + "B".center(8) + "M".center(8) + "4s".center(8) + "6s".center(
                    8) + "SR".center(8) + "\n\n")

            for j in range(0, len(self.nick1)):

                count = 0

                for i in range(0, 11):
                    if self.batting1[self.squad1[i]][0] != "" and self.batting1[self.squad1[i]][0] == self.nick1[j]:
                        count = count + 1
                        batt[self.squad1[i]] = self.batting1[self.squad1[i]]
                        print(self.batting1[self.squad1[i]][1].rstrip().ljust(40),
                              self.batting1[self.squad1[i]][2].center(8), self.batting1[self.squad1[i]][3].center(8),
                              self.batting1[self.squad1[i]][4].center(8), self.batting1[self.squad1[i]][5].center(8),
                              self.batting1[self.squad1[i]][6].center(8), self.batting1[self.squad1[i]][7].center(8))

                        file1.write(self.batting1[self.squad1[i]][1].rstrip().ljust(50) + self.batting1[self.squad1[i]][
                            2].center(8) + self.batting1[self.squad1[i]][3].center(8) + self.batting1[self.squad1[i]][
                                        4].center(8) + self.batting1[self.squad1[i]][5].center(8) +
                                    self.batting1[self.squad1[i]][6].center(8) + self.batting1[self.squad1[i]][
                                        7].center(8) + "\n")
                        file2.write(self.batting1[self.squad1[i]][1].rstrip().ljust(50) + self.batting1[self.squad1[i]][
                            2].center(8) + self.batting1[self.squad1[i]][3].center(8) + self.batting1[self.squad1[i]][
                                        4].center(8) + self.batting1[self.squad1[i]][5].center(8) +
                                    self.batting1[self.squad1[i]][6].center(8) + self.batting1[self.squad1[i]][
                                        7].center(8) + "\n")

            q = set(self.batting1) - set(batt)
            for val in q:
                s = s + val.rstrip() + " , "
            print("EXTRA".ljust(30), self.extra.center(20))
            file1.write("\nEXTRA".ljust(30) + self.extra.center(20))
            file2.write("\nEXTRA".ljust(30) + self.extra.center(20))
            print("TOTAL".ljust(30), self.tr.center(20))
            file1.write("\nTOTAL".ljust(30) + self.tr.center(20))
            file2.write("\nTOTAL".ljust(30) + self.tr.center(20))
            print("FOW:  ", fow())
            print(s
                  )
            file1.write("\nFOW:  " + fow())
            file1.write(s)
            file2.write("\nFOW:  " + fow())
            file2.write(s)

            # print("batsman".ljust(40),"R".center(8),"B".center("8"),"M".center(8),"4s".center(8),"6s".center(8),"SR".center(8))

    class bowling:
        squad1 = []
        squad2 = []
        nick1 = []
        bowling1 = {}

        def nick(self):
            f = open(m)
            for i in range(0, 11):
                self.bowling1[self.squad1[i]] = ["", "", "", "", "", "", "", "", "", "", "", self.squad1[i]]

            for l in f:
                if "##########" not in l:

                    b = re.search('[A-Z][a-z]+ to [A-Z]*[a-z]+', l)
                    if b != None:
                        x, y = b.group().split(" to ")

                        for i in range(0, 11):
                            if x in self.squad1[i] and self.bowling1[self.squad1[i]][0] == "":
                                for j in range(0, 11):
                                    if y in self.squad2[j]:
                                        self.nick1.append(x)

                                        self.bowling1[self.squad1[i]][0] = x
                                        break





                else:
                    f.close()
                    break
            # self.nick1.reverse()

        def bowl(self):

            for i in range(0, 11):
                co = self.bowling1[self.squad1[i]][0]
                f = open(m)
                r = 0
                mc = 0
                b = 0
                d = 0
                nf = 0
                ns = 0
                ec = 0
                wk = 0
                wi = 0
                nb = 0
                lb = 0
                cm = 0
                wb = 0
                for l in f:

                    if "##########" not in l:
                        bi = re.search('[a-z]*[A-Z]*[a-z]+ to [A-z]*[a-z]+', l)
                        if bi != None:
                            x, y = bi.group().split(" to ")

                            if x == (co):

                                e = re.sub(r"\s+", "", l).split(",")

                                if "1run" in e:
                                    r = r + 1
                                    b = b + 1
                                    cm = 0

                                elif "2runs" in e:
                                    r = r + 2
                                    b = b + 1
                                    cm = 0
                                elif "3runs" in e:
                                    r = r + 3
                                    b = b + 1
                                    cm = 0
                                elif "FOURruns" in e or "4runs" in e:
                                    r = r + 4
                                    b = b + 1
                                    nf = nf + 1
                                    cm = 0
                                elif "SIXruns" in e or "6runs" in e:
                                    r = r + 6
                                    b = b + 1
                                    ns = ns + 1
                                    cm = 0
                                elif "norun" in e:
                                    b = b + 1
                                    d = d + 1
                                    cm = cm + 1
                                elif "1wide" in e:
                                    r = r + 1
                                    wi = wi + 1
                                    cm = 0
                                    wb = wb + 1
                                elif "2wides" in e:
                                    r = r + 2
                                    wi = wi + 2
                                    cm = 0
                                    wb = wb + 1
                                elif "3wides" in e:
                                    r = r + 3
                                    wi = wi + 3
                                    wb = wb + 1
                                elif "4wides" in e:
                                    r = r + 4
                                    wi = wi + 4
                                    cm = 0
                                    wb = wb + 1
                                elif "5wides" in e:
                                    r = r + 5
                                    nf = nf + 1
                                    wi = wi + 5
                                    cm = 0
                                    wb = wb + 1
                                elif "(noball)FOURruns" in e:

                                    r = r + 5
                                    nf = nf + 1

                                    nb = nb + 1
                                    cm = 0

                                elif "(noball)" in e or "1noball" in e:
                                    r = r + 1
                                    cm = 0

                                    nb = nb + 1
                                elif "(noball)1run" in e:

                                    r = r + 2
                                    cm = 0

                                    nb = nb + 1
                                elif "(noball)2runs" in e:

                                    r = r + 3
                                    nb = nb + 1
                                    cm = 0

                                elif "(noball)3runs" in e:

                                    r = r + 4
                                    cm = 0

                                    nb = nb + 1
                                elif "(noball)SIXruns" in e:

                                    r = r + 7
                                    nb = nb + 1
                                    cm = 0

                                    ns = ns + 1
                                elif "OUT" in e:
                                    b = b + 1
                                    wk = wk + 1
                                    d = d + 1
                                    cm = cm + 1
                                elif "1runOUT" in e:
                                    b = b + 1
                                    r = r + 1
                                    # wk = wk+1


                                elif "1legbye" in e or "1bye" in e:
                                    b = b + 1
                                    lb = lb + 1
                                    d = d + 1
                                    cm = cm + 1

                                elif "2legbyes" in e or "2byes" in e:
                                    b = b + 1
                                    lb = lb + 2
                                    d = d + 1
                                    cm = cm + 1

                                elif "3legbyes" in e or "3byes" in e:
                                    b = b + 1
                                    lb = lb + 3
                                    d = d + 1
                                    cm = cm + 1

                                elif "FOURlegbyes" in e or "4legbyes" in e or "4byes" in e:
                                    b = b + 1
                                    lb = lb + 4
                                    d = d + 1
                                    cm = cm + 1
                                elif "5runs" in e:
                                    r = r + 5
                                    b = b + 1
                                    cm = 0
                                elif "6runs" in e:
                                    r = r + 6
                                    b = b + 1
                                    cm = 0

                                if cm == 6:
                                    mc = mc + 1
                                    cm = 0

                            else:
                                for j in range(0, 11):
                                    if y in self.squad2[j]:
                                        cm = 0



                    else:

                        self.bowling1[self.squad1[i]][1] = str(float(str(b // 6) + "." + str(b % 6)))
                        self.bowling1[self.squad1[i]][2] = str(mc)
                        self.bowling1[self.squad1[i]][3] = str(r)
                        self.bowling1[self.squad1[i]][4] = str(wk)
                        if b != 0:
                            self.bowling1[self.squad1[i]][5] = str(round((r / b) * 6, 2))
                        self.bowling1[self.squad1[i]][6] = str(d)
                        self.bowling1[self.squad1[i]][7] = str(nf)
                        self.bowling1[self.squad1[i]][8] = str(ns)
                        self.bowling1[self.squad1[i]][9] = str(wb)
                        self.bowling1[self.squad1[i]][10] = str(nb)
                        f.close()

                        break

        def bowlcard(self):

            bowl = {}
            print("bowling".ljust(20), "O".center(8), "M".center(8), "R".center(8), "W".center(8), "Econ".center(8),
                  "0s".center(8), "4s".center(8), "6s".center(8), "WD".center(8), "NB".center(8))

            file1.write(
                "\n\nbowling".ljust(20) + "O".center(8) + "M".center(8) + "R".center(8) + "W".center(8) + "Econ".center(
                    8) + "0s".center(8) + "4s".center(8) + "6s".center(8) + "WD".center(8) + "NB".center(8) + "\n\n\n")
            file2.write(
                "\n\nbowling".ljust(20) + "O".center(8) + "M".center(8) + "R".center(8) + "W".center(8) + "Econ".center(
                    8) + "0s".center(8) + "4s".center(8) + "6s".center(8) + "WD".center(8) + "NB".center(8) + "\n\n\n")
            for j in range(0, len(self.nick1)):

                for i in range(0, 11):
                    if self.bowling1[self.squad1[i]][0] != "" and self.bowling1[self.squad1[i]][0] == self.nick1[j] and \
                            self.bowling1[self.squad1[i]][1] != "0.0":
                        bowl[self.squad1[i]] = self.bowling1[self.squad1[i]]
                        print(self.bowling1[self.squad1[i]][11].rstrip().ljust(20),
                              self.bowling1[self.squad1[i]][1].center(8), self.bowling1[self.squad1[i]][2].center(8),
                              self.bowling1[self.squad1[i]][3].center(8), self.bowling1[self.squad1[i]][4].center(8),
                              self.bowling1[self.squad1[i]][5].center(8), self.bowling1[self.squad1[i]][6].center(8),
                              self.bowling1[self.squad1[i]][7].center(8), self.bowling1[self.squad1[i]][8].center(8),
                              self.bowling1[self.squad1[i]][9].center(8), self.bowling1[self.squad1[i]][10].center(8))
                        file1.write(
                            self.bowling1[self.squad1[i]][11].rstrip().ljust(20) + self.bowling1[self.squad1[i]][
                                1].center(8) + self.bowling1[self.squad1[i]][2].center(8) +
                            self.bowling1[self.squad1[i]][3].center(8) + self.bowling1[self.squad1[i]][4].center(8) +
                            self.bowling1[self.squad1[i]][5].center(8) + self.bowling1[self.squad1[i]][6].center(8) +
                            self.bowling1[self.squad1[i]][7].center(8) + self.bowling1[self.squad1[i]][8].center(8) +
                            self.bowling1[self.squad1[i]][9].center(8) + self.bowling1[self.squad1[i]][10].center(
                                8) + "\n")
                        file2.write(
                            self.bowling1[self.squad1[i]][11].rstrip().ljust(20) + self.bowling1[self.squad1[i]][
                                1].center(8) + self.bowling1[self.squad1[i]][2].center(8) +
                            self.bowling1[self.squad1[i]][3].center(8) + self.bowling1[self.squad1[i]][4].center(8) +
                            self.bowling1[self.squad1[i]][5].center(8) + self.bowling1[self.squad1[i]][6].center(8) +
                            self.bowling1[self.squad1[i]][7].center(8) + self.bowling1[self.squad1[i]][8].center(8) +
                            self.bowling1[self.squad1[i]][9].center(8) + self.bowling1[self.squad1[i]][10].center(
                                8) + "\n")

    def split(fl):

        f1 = open(fl + "_com(kafka).txt")
        f2 = open(fl + "(1).txt", "w")
        f3 = open(fl + "(2).txt", "w")
        for l in f1:
            if "@@" not in l:
                f3.write(l)
            else:
                break

        for l in f1:

            if "@@@@@@@@@@" not in l:
                f2.write(l)
            else:


                break
        f2.write("##########")
        for l in f1:
            if "##########" not in l:
                f3.write(l)
            else:

                break
        f3.write("##########")
        f1.close()
        f2.close()
        f3.close()

    split(inp)
    m = inp + "(2).txt";

    p1 = batting()
    p2 = bowling()
    p1.squad1, p1.squad2 = getsquad()
    p2.squad2, p2.squad1 = getsquad()
    p1.nick()

    p2.nick()

    p1.batt()
    p2.bowl()

    print("\nIST INNING MATCH " + inp + "\n")
    m = inp + "(1).txt"
    p3 = batting()
    p3.batting1 = {}
    p3.nick1 = []
    p3.squad1 = p2.squad1
    p3.squad2 = p2.squad2
    p4 = bowling()
    p4.bowling1 = {}
    p4.nick1 = []
    p4.squad1 = p1.squad1
    p4.squad2 = p1.squad2
    p3.nick()
    p4.nick()
    p3.batt()
    p3.batcard()
    p4.bowl()
    p4.bowlcard()
    print("\nIIND INNING MATCH " + inp + "\n")

    file1.write("\nIIND INNING MATCH " + inp + "\n")
    file2.write("\nIIND INNING MATCH " + inp + "\n")
    m = inp + "(2).txt"
    p1.batcard()
    p2.bowlcard()

    file1.write("----------------------------------------------------------------------------------------------------------------------------\n")
    file1.close()
    file2.close()


consumers = []
rconsumers = []
files = []
thrd = []
fid = ["194161014-4143","194161014-4144","194161014-4145","194161014-4146","194161014-4147","194161014-4148","194161014-4149","194161014-4150","194161014-4151","194161014-4152","194161014-4153","194161014-4154","194161014-4155","194161014-4157","194161014-4158","194161014-4159","194161014-4160","194161014-4161","194161014-4162","194161014-4163","194161014-4165","194161014-4166","194161014-4168","194161014-4169","194161014-4170","194161014-4171","194161014-4172","194161014-4173","194161014-4174","194161014-4175","194161014-4176","194161014-4177","194161014-4178","194161014-4179","194161014-4180","194161014-4182","194161014-4183","194161014-4184","194161014-4186","194161014-4187","194161014-4188","194161014-4190","194161014-4191","194161014-4192"]

for i in range(0, 44):
    consumer = KafkaConsumer(fid[i])
    consumers.append(consumer)
    file = open(fid[i] + "_com(kafka).txt", "w")
    files.append(file)
    thrd.append(threading.Thread(target=thread_function, args=(i,)))
    thrd[i].start()

fopen = files