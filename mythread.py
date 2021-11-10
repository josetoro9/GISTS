import threading
import time
import os
import pyodbc
import io
import pandas as pd


CONN_STR = "DSN=impala-prod"

cn = pyodbc.connect( CONN_STR, autocommit = True )

proceso_especifico = True

sqls = []


def query(filename, start=0):
    # Abre y Cierra el archivo
    fd = io.open(filename, 'r', encoding="utf8")
    sqlFile = fd.read()
    fd.close()

    # Split Commands by ";"
    sqlCommands = sqlFile.split(';')
    sqlCommands = sqlCommands[start:-1]

    # Execute every command from the input file
    i = 0;
    fallas = []

    for command in sqlCommands:

        pos_tb = command.find('tabla')
        pos_cd = max(command.find('create'), command.find('drop'))
        try:
            cn.execute(command)
        except  Exception as e:
            e_msg = e
            fallas.append('\nQuery ' + command[pos_cd:pos_cd + 60] + '...\n' + str(e_msg))  # Bad queries
        i = i + 1

    return fallas

def define_query(query):
    resultado = [] 
    temp = cn.execute(query).fetchall()

    largo_data=len(temp)-18
    nombre_creador=temp[largo_data]

    largo_file=len(query)
    tabla=query[19:largo_file]

    resultado.append(tabla + ',' + nombre_creador)
    return resultado


class MyThread(threading.Thread):

    def __init__(self, num_thread=0, name_sql=""):
        threading.Thread.__init__(self)
        self.num_thread = num_thread
        self.name_sql = name_sql

    def run(self):
        print("{} started!".format(self.num_thread))
        print("Executing sql: {}".format(self.name_sql))

        if proceso_especifico == True:
            creadores = define_query(self.name_sql)
        else:
            query(self.name_sql) 

        print("{} finished!".format(self.num_thread))
        return creadores

def creacion_sql(data_base):
    q='show tables in  ' + data_base

    a = cn.execute(q).fetchall()

    for row in a:
    
        sqls.append('describe formatted ' + data_base + '.' + row[0])

    return sqls
  
def main():
    db='proceso_clientes_personas_y_pymes'
    creacion_sql(db)
    for x in range(len(sqls)):
        mythread = MyThread(num_thread=x,
                            name_sql=sqls[x])
        print("\nPrueba orden item: " + str(x))
        mythread.start()
        mythread_1 = MyThread(0,filename_1)
        mythread_2 = MyThread(0,filename_2)
        mythread_3 = MyThread(0,filename_3)
        mythread_4 = MyThread(0,filename_4)
        mythread_1.start()
        mythread_2.start()
        mythread_3.start()
        mythread_4.start()

    creadores=creadores
    
    
    cn.close()


if __name__ == '__main__':
    main()



