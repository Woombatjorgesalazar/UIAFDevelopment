/*Funcion de consulta de columnas*/
CREATE OR REPLACE FUNCTION public.consult_columnas(nombre_tabla character varying, tipoc character varying)
 RETURNS TABLE(nombre_columna character varying)
 LANGUAGE plpgsql
AS $function$
begin
	if tipoc = 'tabla' then
		return query
		select tca.nombre as nombre_campos
		from tcatalogo tc
		inner join tcampo tca on tc.idcatalogo = tca.idcatalogo
		where tc.nombrecatalogo = nombre_tabla;
	elsif tipoc = 'idtabla' then
		return query
		select tca.nombre as nombre_campos
		from tcatalogo tc
		inner join tcampo tca on tc.idcatalogo = tca.idcatalogo 
		where tc.idcatalogo = cast(nombre_tabla as int);
	end if;
END;
$function$
;

/*Consulta de ETL*/
CREATE OR REPLACE FUNCTION public.consult_etl(tabla character varying)
 RETURNS TABLE(proceso character varying, ruta_etl character varying)
 LANGUAGE plpgsql
AS $function$
	BEGIN
		return query
		select t3.proceso, t3.rutaetl
		from tcatalogo t
		inner join tprocesocatalogo t2
		on t.idcatalogo = t2.idcatalogo
		inner join tproceso t3 
		on t2.idproceso = t3.idproceso 
		where nombrecatalogo = tabla;
		
		
		
	END;
$function$
;


/*Consulta de tablas relacionadas*/
CREATE OR REPLACE FUNCTION public.consulta_tablas_rel(nombre_tabla character varying, tipoc character varying)
 RETURNS TABLE(nombre_catalogo character varying, nombre_catalogo_2 character varying, nombre character varying, idcatalogo integer, idcampo double precision)
 LANGUAGE plpgsql
AS $function$
begin
	if tipoc = 'tabla' then
		return query
		select tca.nombrecatalogo, tca2.nombrecatalogo, tc.nombre, cast(tr.idcatalogoorigen as integer) as idcatalogo , cast(tr.idcampoorigen as float) as idcampo
		from trelacion tr 
		inner join tcampo tc on tr.idcatalogodestino = tc.idcatalogo and tr.idcampodestino = tc.idcampo
		inner join tcatalogo tca on tc.idcatalogo = tca.idcatalogo 
		inner join tcatalogo tca2 on tr.idcatalogoorigen = tca2.idcatalogo 
		where tca.nombrecatalogo = nombre_tabla;
	elsif tipoc = 'idtabla' then
		return query
		select tca.nombrecatalogo, tca2.nombrecatalogo, tc.nombre, cast(tr.idcatalogoorigen as integer) as idcatalogo , cast(tr.idcampoorigen as float) as idcampo
		from trelacion tr 
		inner join tcampo tc on tr.idcatalogodestino = tc.idcatalogo and tr.idcampodestino = tc.idcampo
		inner join tcatalogo tca on tc.idcatalogo = tca.idcatalogo 
		inner join tcatalogo tca2 on tr.idcatalogoorigen = tca2.idcatalogo 
		where tca.idcatalogo = cast(nombre_tabla as int);
	end if;
		
END;
$function$
;


/*Cruze de tablas*/
CREATE OR REPLACE FUNCTION public.cruze_tablas(tabla_origen character varying, tabla_destino character varying)
 RETURNS TABLE(campo_origen character varying, campo_destino character varying)
 LANGUAGE plpgsql
AS $function$
begin
	return query
	select t2.nombre as campo_origen, t3.nombre as campo_destino
	from tcatalogo as torigen
	inner join trelacion
	on torigen.idcatalogo =  trelacion.idcatalogoorigen
	inner join tcatalogo tdestino
	on trelacion.idcatalogodestino = tdestino.idcatalogo
	inner join tcampo t2
	on trelacion.idcampoorigen = t2.idcampo
	inner join tcampo t3
	on trelacion.idcampodestino = t3.idcampo
	where torigen.nombrecatalogo = tabla_origen and tdestino.nombrecatalogo = tabla_destino;

END;
$function$
;


/* Obtencio de proceso para paso 2*/
CREATE OR REPLACE FUNCTION public.get_procepaso_2(p_sigla character varying)
 RETURNS TABLE(idproceso integer, proceso character varying, idestadoproceso integer, idperiodicidad integer, nombrecatalogo character varying, idcatalogo integer, idtipocatalogo integer, separador character, origen character, tipocatalogo character varying, basedatos character varying, servidor character varying, idsistema integer)
 LANGUAGE plpgsql
AS $function$              
begin
	 return query
 	 select pro.idproceso ,pro.proceso ,PRO.idestadoproceso, PRO.idperiodicidad, 
		cat.nombrecatalogo,cat.idcatalogo, cat.idtipocatalogo ,cat.separador ,procat.origen, ttipocatalogo.tipocatalogo,
		sis.basedatos, sis.servidor, cat.idsistema 
		from tproceso pro  
		inner join tprocesocatalogo procat
		on pro.idproceso =procat.idproceso 
		inner join tcatalogo cat
		on procat.idproceso = pro.idproceso 
		and procat.idcatalogo =cat.idcatalogo 
		inner join ttipocatalogo 
		on cat.idtipocatalogo =ttipocatalogo.idtipocatalogo 
		inner join tsistema sis
		on cat.idsistema = sis.idsistema 
		where pro.sigla= p_sigla and pro.paso=2;
END;
$function$
;


/*get proceso antigua*/
CREATE OR REPLACE FUNCTION public.get_proceso(p_sigla character varying)
 RETURNS TABLE(idproceso integer, proceso character varying, idestadoproceso integer, idperiodicidad integer, nombrecatalogo character varying, idcatalogo integer, idtipocatalogo integer, separador character, origen character, tipocatalogo character varying, basedatos character varying, servidor character varying)
 LANGUAGE plpgsql
AS $function$              
begin
	 return query
 	 select pro.idproceso ,pro.proceso ,PRO.idestadoproceso, PRO.idperiodicidad, 
		cat.nombrecatalogo,cat.idcatalogo, cat.idtipocatalogo ,cat.separador ,procat.origen, ttipocatalogo.tipocatalogo,
		sis.basedatos, sis.servidor 
		from tproceso pro  
		inner join tprocesocatalogo procat
		on pro.idproceso =procat.idproceso 
		inner join tcatalogo cat
		on procat.idproceso = pro.idproceso 
		and procat.idcatalogo =cat.idcatalogo 
		inner join ttipocatalogo 
		on cat.idtipocatalogo =ttipocatalogo.idtipocatalogo 
		inner join tsistema sis
		on cat.idsistema = sis.idsistema 
		where pro.sigla= p_sigla and pro.paso=1;
END;
$function$
;

/* Marcar fin antigua*/
CREATE OR REPLACE FUNCTION public.marcar_fin(int_idcargue integer, int_estado integer, fecha timestamp without time zone DEFAULT now(), int_registros integer DEFAULT 0, varc_observacion character varying DEFAULT ''::character varying)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$  
declare date_incio timestamp;

begin
		
	select fechaejecucion  into  date_incio
	from tcargue 
	where idcargue=int_idcargue;
	if varc_observacion='' then 
			if int_estado=2 then
				varc_observacion='Finalizado correctamente';
			else	
				varc_observacion='Finalizdo con error';
			end if;
	 end if;
		
	 update tcargue set duracion =minutos(date_incio,fecha),fechafin =fecha,
			registroscargados=int_registros, estado =int_estado,observacion =varc_observacion
		where idcargue =int_idcargue;

	return int_idcargue;
END;
$function$
;


/*Marcar inicio antigua*/
CREATE OR REPLACE FUNCTION public.marcar_inicio(int_idproceso integer, dat_fechaejecucion timestamp without time zone DEFAULT now(), str_observacion character varying DEFAULT ''::character varying)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$  
declare int_idcargue int=0; 
begin
	
	if (select EXISTS(select idcargue from tcargue 
			where idproceso=int_idproceso and estado=1)) then 
			
		select idcargue into   int_idcargue  
		from tcargue 
		where idproceso=int_idproceso and estado=1;
		
	end if;

	if (int_idcargue = 0) then
		if str_observacion=''then
			str_observacion='proceso iniciado';
		end if;
		insert into tcargue (idproceso,fechaejecucion,estado,observacion )
		values(int_idproceso,dat_fechaejecucion,1,str_observacion);
	
		SELECT currval(pg_get_serial_sequence('tcargue', 'idcargue')) into int_idcargue;
		
	end if;


	return int_idcargue;
END;
$function$
;


/*minutos antigua*/
CREATE OR REPLACE FUNCTION public.minutos(fecha1 timestamp without time zone, fecha2 timestamp without time zone)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
	begin 
  
		return (SELECT (DATE_PART('day', fecha2::timestamp - fecha1::timestamp) * 24 + 
		               DATE_PART('hour', fecha2::timestamp - fecha1::timestamp)) * 60 +
		               DATE_PART('minute', fecha2::timestamp - fecha1::timestamp));
    end ;
	$function$
;


/*Resultado de cargue*/
CREATE OR REPLACE FUNCTION public.result_cargue(nombre_tabla character varying, tipo character varying)
 RETURNS TABLE(idcargue integer, idproceso integer, fechaejecucion timestamp without time zone, duracion integer, fechafin timestamp without time zone, registroscargados integer, estado integer, observacion character varying)
 LANGUAGE plpgsql
AS $function$
begin
	if tipo = 'tabla' then
		return query
		select t4.*
		from tcatalogo t
		inner join tprocesocatalogo t2 
		on t.idcatalogo = t2.idcatalogo 
		inner join tproceso t3 
		on t2.idproceso = t3.idproceso 
		inner join tcargue t4 
		on t3.idproceso = t4.idproceso 
		where t.nombrecatalogo = nombre_tabla and t4.idcargue = (select max(tcargue.idcargue)
				from tcatalogo
				inner join tprocesocatalogo 
				on tcatalogo .idcatalogo = tprocesocatalogo.idcatalogo 
				inner join tproceso 
				on tprocesocatalogo.idproceso = tproceso.idproceso 
				inner join tcargue
				on tproceso.idproceso = tcargue.idproceso 
				where tcatalogo.nombrecatalogo = nombre_tabla);
	elsif tipo = 'idtabla' then
		return query
		select t4.*
		from tcatalogo t
		inner join tprocesocatalogo t2 
		on t.idcatalogo = t2.idcatalogo 
		inner join tproceso t3 
		on t2.idproceso = t3.idproceso 
		inner join tcargue t4 
		on t3.idproceso = t4.idproceso 
		where t.idcatalogo = cast(nombre_tabla as int) and t4.idcargue = (select max(tcargue.idcargue)
				from tcatalogo
				inner join tprocesocatalogo 
				on tcatalogo .idcatalogo = tprocesocatalogo.idcatalogo 
				inner join tproceso 
				on tprocesocatalogo.idproceso = tproceso.idproceso 
				inner join tcargue
				on tproceso.idproceso = tcargue.idproceso 
				where tcatalogo.idcatalogo = cast(nombre_tabla as int));
	end if;
end;
$function$
;


