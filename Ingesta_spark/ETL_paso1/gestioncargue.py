import common as cm

def marcar_inicio (Hc,idproceso, fecha,observacion=""):
    sql ="(select * from public.marcar_inicio(%s, '%s','%s')) as idcargue" % (idproceso,fecha,observacion)
    cm.verbose_c(sql,3)
    idcargue= cm.get_Query_Postgresql(Hc, sql).select("marcar_inicio").collect()[0]["marcar_inicio"]
    return idcargue

def marcar_fin(Hc,idcargue,estado,fecha,registros,observacion):
    observacion = observacion.replace("'", " ")
    sql ="(SELECT * from public.marcar_fin(%s,%s,'%s',%s,'%s'))as idcargue" %(idcargue,estado,fecha,registros,observacion);
    cm.verbose_c(sql, 3)
    idcargue= cm.get_Query_Postgresql(Hc, sql).select ("marcar_fin").collect()[0]["marcar_fin"]
    return idcargue

def marcar_inicio_cargue_archivo  (Hc,idproceso, fecha,observacion=""):
    #busco elnumero del proceso del cargue de archivo
<<<<<<< HEAD:ETL_paso1/gestioncargue.py
    sql ="(select idproceso from tproceso where idprocesopadre =%s and idproceso <>%s) as idproceso" %(idproceso,idproceso)
    try:
        idproceso_a = cm.get_Query_Postgresql(Hc, sql).select("idproceso").collect()[0]["idproceso"]
    except IndexError:
        idproceso_a = idproceso
=======
    sql ="(select idproceso from tproceso where idproceso =%s) as idproceso" %(idproceso)

    idproceso_a = cm.get_Query_Postgresql(Hc, sql).select("idproceso").collect()[0]["idproceso"]
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589:gestioncargue.py
    idcargue=marcar_inicio(Hc, idproceso_a, fecha,observacion)
    return idcargue

def marcar_fin_cargue_archivo  (Hc,idcargue,estado,fecha,registros,observacion):
    marcar_fin(Hc, idcargue, estado, fecha, registros, observacion)

