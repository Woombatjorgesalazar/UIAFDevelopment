from time import gmtime, strftime, localtime, time, mktime, strptime, sleep
class Festivos:
    def __init__(self, ano):
        self.__festivos=[]
        self.__hoy=strftime("%d/%m/%Y")
        
        if ano == "":
            ano=strftime("%Y")
        
        self.__ano=ano
        pascua = self.__pascua(ano)
        self.__pascua_dia=pascua[0]
        self.__pascua_mes=pascua[1]
        primero=(ano,1,1)
        self.__festivos.append(primero)         #primero de enero
        trabajo=(ano,5,1)
        self.__festivos.append(trabajo)         #dia del trabajo
        independencia=(ano,7,20)
        self.__festivos.append(independencia)   #independecia de colombia
        boyaca=(ano,8,7)
        self.__festivos.append(boyaca)          #batalla de boyaca
        virgen=(ano,12,8)
        self.__festivos.append(virgen)          #dia de la velitas inmaculada concepcion
        navidad=(ano,12,25)
        self.__festivos.append(navidad)         #navidad

        self.__calcula_emiliani(1,6)            #reyes magos
        self.__calcula_emiliani(3,19)           #san jose
        self.__calcula_emiliani(6,29)           #San pedro y san pablo
        self.__calcula_emiliani(8,15)           #Asuncion de la Virgen
        self.__calcula_emiliani(10,12)          #Dia de la Raza
        self.__calcula_emiliani(11,1)           #Todos los Santos
        self.__calcula_emiliani(11,11)          #Independencia de Cartagena

        self.__otrasFechasCalculadas(-3)        #Jueves Santo
        self.__otrasFechasCalculadas(-2)        #Viernes Santo

        self.__otrasFechasCalculadas(43,True)   #Ascension de Jesus
        self.__otrasFechasCalculadas(64,True)   #Corpus Christi
        self.__otrasFechasCalculadas(71,True)   #Sagrado Corazon de Jesus

    def __calcula_emiliani(self, mes_festivo, dia_festivo):
        t = (self.__ano, mes_festivo, dia_festivo, 0, 0, 0, 0, 0, 0)
        dd =int(strftime("%w", localtime(mktime(t))))
        if dd == 0:
            dia_festivo = dia_festivo + 1
        elif dd == 2:
            dia_festivo = dia_festivo + 6
        elif dd == 3:
            dia_festivo = dia_festivo + 5
        elif dd == 4:
            dia_festivo = dia_festivo + 4
        elif dd == 5:
            dia_festivo = dia_festivo + 3
        elif dd == 6:
            dia_festivo = dia_festivo + 2
        t = (self.__ano, mes_festivo, dia_festivo , 0, 0, 0, 0, 0, 0)
        mes=int(strftime("%m", localtime(mktime(t))))
        dia=int(strftime("%d", localtime(mktime(t))))
        festivo=(self.__ano,mes,dia)
        self.__festivos.append(festivo)

    def __otrasFechasCalculadas(self, cantidadDias=0,siguienteLunes=False):
        suma = int(self.__pascua_dia)+int(cantidadDias)
        t = (self.__ano, self.__pascua_mes, suma, 0, 0, 0, 0, 0, 0)
        mes_festivo=int(strftime("%m", localtime(mktime(t))))
        dia_festivo=int(strftime("%d", localtime(mktime(t))))
        if siguienteLunes:
            self.__calcula_emiliani(mes_festivo, dia_festivo)
        else:
            festivo=(self.__ano,mes_festivo,dia_festivo)
            self.__festivos.append(festivo)
    
    def __pascua(self, anno):
        M = 24  
        N = 5
        a = anno % 19
        b = anno % 4
        c = anno % 7
        d = (19*a + M) % 30
        e = (2*b+4*c+6*d + N) % 7

        if d+e < 10  :
            dia = d+e+22
            mes = 3
        else:
            dia = d+e-9
            mes = 4

        if dia == 26  and mes == 4:
            dia = 19

        if dia == 25 and mes == 4 and d==28 and e == 6 and a >10:
            dia = 18

        return [dia, mes, anno]
    
    def esFestivo(self, mes, dia):
        return (self.__ano,mes,dia) in self.__festivos