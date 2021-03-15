select * from tcatalogo t ;


select * from tproceso t ;
select * from tsistema;

select t2.* from tcatalogo t1
inner join tproceso t2
on t1.nombrecatalogo = t2.sigla 
where t1.nombrecatalogo = 'TE10';

select * from tprocesocatalogo t ;

select * from tcargue;

select * from get_proceso('TE10');

select * from consult_columnas('TE10_ENCABEZADO','tabla');
select * from tsistema;
select * from tcatalogo t2 ;
select * from ttipocatalogo t2 ;
select * from ttipocampo t2 ;
insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimproducto', (select idsistema from tsistema where nombre='dwh_uiaf'), '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idproducto', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'identidad', 'int', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsector', 'int', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'iddaneapertura', 'string', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idfechaapertura', 'int', 0, 5),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idestadoproducto', 'int', 0, 6),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idtipoproducto', 'string', 0, 7),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'numero', 'string', 0, 8);

select * from tcatalogo t2 ;
select * from ttipocampo t2 ;
insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimproductosistema', (select idsistema from tsistema where nombre='dwh_uiaf'), '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idproductosistema', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idpersona', 'int', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'identidad', 'int', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsector', 'int', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'iddaneapertura', 'string', 0, 5),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idfechaapertura', 'int', 0, 6),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idestadoproducto', 'int', 0, 7),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idcatalogo', 'int', 0, 8),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsistema', 'int', 0, 9),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idproducto', 'int', 0, 10),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idfechacargue', 'int', 0, 11),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'identregatransaccional', 'int', 0, 12),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'numeroregtransaccional', 'int', 0, 13),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'estado', 'int', 0, 14),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idtipoproducto', 'string', 0, 15),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idfechatransaccion', 'int', 0, 16),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'numeroproductotransaccion', 'string', 0, 17),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'numero', 'string', 0, 18);

select * from ttipocatalogo t3 ;
select * from tcatalogo t2 ;
select * from ttipocampo t2 ;
insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (3, 'dimfecha', (select idsistema from tsistema where nombre='dwh_uiaf'), '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idfecha', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'fecha', 'string', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'diames', 'int', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombredia', 'string', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'mesanno', 'string', 0, 5),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombremes', 'string', 0, 6),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'festivo', 'int', 0, 7);
	
select * from ttipocatalogo t3 ;
select * from tcatalogo t2 ;
select * from ttipocampo t2 ;
insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimsector', (select idsistema from tsistema where nombre='dwh_uiaf'), '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsector', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombresector', 'string', 0, 2);

select * from ttipocatalogo t3 ;
select * from tcatalogo t2 ;
select * from ttipocampo t2 ;

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimsectordetallado', (select idsistema from tsistema where nombre='dwh_uiaf'), '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idsectordetallado', 'string', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsistema', 'int', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idcatalogo', 'int', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsector', 'int', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'sectorsistema', 'string', 0, 5),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'idsectorsistema', 'string', 0, 6),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombresector', 'string', 0, 7);

select * from ttipocatalogo t3 ;
select * from tcatalogo t2 ;
select * from ttipocampo t2 ;
insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimentidad', (select idsistema from tsistema where nombre='dwh_uiaf'), '', 'SIN', 'F');


insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'identidad', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'identificacion', 'string', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'iddaneubicacion', 'string', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'direccion', 'string', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'telefono', 'string', 0, 5),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'correoelectronico', 'string', 0, 6),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'razonsocial', 'string', 0, 7),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'fechadecreacion', 'string', 0, 8),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'identidadprincipal', 'int', 0, 9),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'idpersonaoc', 'int', 0, 10),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idactividadeconomica', 'int', 0, 11),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idpersona', 'int', 0, 12),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idtipoidentificacion', 'int', 0, 13),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsector', 'int', 0, 14),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsupervisor', 'int', 0, 15),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombresupervisor', 'string', 0,16),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombresector', 'string', 0, 17);


select * from ttipocatalogo t3 ;
select * from tcatalogo t2 ;
select * from ttipocampo t2 ;
insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimentidadsistema', (select idsistema from tsistema where nombre='dwh_uiaf'), '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'identidadsistema', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'identidad', 'int', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsector', 'int', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'identificacion', 'string', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'identificacionsistema', 'string', 0, 5),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'iddaneubicacion', 'string', 0, 6),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'direccion', 'string', 0, 7),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'direccionsistema', 'string', 0, 8),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'telefono', 'string', 0, 9),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'telefonosistema', 'string', 0, 10),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'correoelectronico', 'string', 0, 11),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'correoelectronicosistema', 'string', 0, 12),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'razonsocial', 'string', 0, 13),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'razonsocialsistema', 'string', 0, 14),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'fechacreacion', 'string', 0, 15),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'fechacreacionsistema', 'string', 0, 16),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'identidadprincipal', 'int', 0, 17),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'idpersonaoc', 'int', 0, 18),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'idactividadeconomica', 'int', 0, 19),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'sectorsistema', 'int', 0, 20),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'tipoentidadsistema', 'int', 0, 21),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'codigoentidadsistema', 'string', 0, 22),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'fechacargue', 'string', 0, 23),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsistema', 'int', 0, 24),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idcatalogo', 'int', 0, 25),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idtipoidentificacion', 'int', 0, 26),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'dim_idsector', 'int', 0, 27),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'idsectordetallado', 'string', 0, 28),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'idsupervisorsistema', 'string', 0, 29);

select * from ttipocatalogo t3 ;
select * from tcatalogo t2 ;
select * from ttipocampo t2 ;
insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimtipotransaccion', (select idsistema from tsistema where nombre='dwh_uiaf'), '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idtipotransaccion', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'tipotransaccion', 'string', 0, 2);	

select * from ttipocatalogo t3 ;
select * from tcatalogo t2 ;
select * from ttipocampo t2 ;
insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimtipotransaccionsistema', (select idsistema from tsistema where nombre='dwh_uiaf'), '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idtipotransaccionsistema', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idtipotransaccion', 'int', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsistema', 'int', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idcatalogo', 'int', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idfechacargue', 'int', 0, 5),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'identregatransaccional', 'int', 0, 6),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'numeroregtransaccional', 'int', 0, 7),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'estado', 'int', 0, 8),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idfechatransaccion', 'int', 0, 9),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'tipotransaccion', 'string', 0, 10),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'tipotransaccionsistema', 'string', 0, 11);


select * from ttipocatalogo t3 ;
select * from tcatalogo t2 ;
select * from ttipocampo t2 ;
insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (3, 'factefectivo', (select idsistema from tsistema where nombre='dwh_uiaf'), '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idefectivo', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idpersonatitular', 'int', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'iddanetransaccion', 'string', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idtipotransaccion', 'int', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'identidadtransaccion', 'int', 0, 5),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsectortransaccion', 'int', 0, 6),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idcatalogo', 'int', 0, 7),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsistema', 'int', 0, 8),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idpersonacontraparte', 'int', 0, 9),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idfecha', 'int', 0, 10),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idproducto', 'int', 0, 11),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idfechacargue', 'int', 0, 12),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'identregatransaccional', 'int', 0, 13),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'numeroregtransaccional', 'int', 0, 14),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'valor', 'float', 0, 15),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'estado', 'int', 0, 16),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idfechatransaccion', 'int', 0, 17),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idmoneda', 'int', 0, 18);

select * from ttipocatalogo t3 ;
select * from tcatalogo t2 ;
select * from ttipocampo t2 ;
select * from ttiporelacion t ;
select * from tcampo t2 where idcatalogo = 86;

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idproducto' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproducto')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproducto'),
	(select idcampo from tcampo where nombre='idproducto' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproductosistema')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproductosistema'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idproducto' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproducto')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproducto'),
	(select idcampo from tcampo where nombre='idproducto' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo'), 2, 'uno a muchos');

/*insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1294, 77, 1192, 78, 2, 'uno a muchos'),
	(1294, 77, 1286, 86, 2, 'uno a muchos'); */


select * from ttipocatalogo t3 ;
select * from tcatalogo t2 ;
select * from ttipocampo t2 ;
select * from ttiporelacion t ;
select * from tcampo t2 where idcatalogo = 82;

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idsector' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimsector')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimsector'),
	(select idcampo from tcampo where nombre='idsector' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimsectordetallado')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimsectordetallado'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idsector' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimsector')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimsector'),
	(select idcampo from tcampo where nombre='idsector' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidadsistema')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidadsistema'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idsector' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimsector')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimsector'),
	(select idcampo from tcampo where nombre='idsector' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad'), 2, 'uno a muchos');
/*
insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1208, 80, 1213, 81, 2, 'uno a muchos'),
	(1208, 80, 1236, 83, 2, 'uno a muchos'),
	(1208, 80, 1230, 82, 2, 'uno a muchos');*/

select * from ttipocatalogo t3 ;
select * from tcatalogo t2 ;
select * from ttipocampo t2 ;
select * from ttiporelacion t ;
select * from tcampo t2 where idcatalogo = 86;

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='identidad' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad'),
	(select idcampo from tcampo where nombre='identidad' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproducto')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproducto'), 2, 'uno a muchos');



insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idsector' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad'),
	(select idcampo from tcampo where nombre='idsector' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproducto')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproducto'), 2, 'uno a muchos');


insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='identidad' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad'),
	(select idcampo from tcampo where nombre='identidad' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidadsistema')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidadsistema'), 2, 'uno a muchos');



insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idsector' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad'),
	(select idcampo from tcampo where nombre='dim_idsector' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidadsistema')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidadsistema'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='identidad' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad'),
	(select idcampo from tcampo where nombre='identidad' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproductosistema')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproductosistema'), 2, 'uno a muchos');



insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idsector' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad'),
	(select idcampo from tcampo where nombre='idsector' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproductosistema')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproductosistema'), 2, 'uno a muchos');



insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='identidad' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad'),
	(select idcampo from tcampo where nombre='identidadtransaccion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo'), 2, 'uno a muchos');



insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idsector' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad'),
	(select idcampo from tcampo where nombre='idsectortransaccion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo'), 2, 'uno a muchos');



/* insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1217, 82, 1295, 77, 2, 'uno a muchos'),
	(1230, 82, 1296, 77, 2, 'uno a muchos'),
	(1217, 82, 1235, 83, 2, 'uno a muchos'),
	(1230, 82, 1260, 83, 2, 'uno a muchos'),
	(1217, 82, 1185, 78, 2, 'uno a muchos'),
	(1230, 82, 1186, 78, 2, 'uno a muchos'),
	(1217, 82, 1280, 86, 2, 'uno a muchos'),
	(1230, 82, 1281, 86, 2, 'uno a muchos');*/


	
select * from ttipocatalogo t3 ;
select * from tcatalogo t2 ;
select * from ttipocampo t2 ;
select * from ttiporelacion t ;
select * from tcampo t2 where idcatalogo = 86;

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idfecha' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimfecha')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimfecha'),
	(select idcampo from tcampo where nombre='idfechaapertura' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproductosistema')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproductosistema'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idfecha' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimfecha')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimfecha'),
	(select idcampo from tcampo where nombre='idfecha' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo'), 2, 'uno a muchos');

/*
insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1201, 79, 1188, 78, 2, 'uno a muchos'),
	(1201, 79, 1285, 86, 2, 'uno a muchos');*/

	
select * from ttipocatalogo t3 ;
select * from tcatalogo t2 ;
select * from ttipocampo t2 ;
select * from ttiporelacion t ;
select * from tcampo t2 where idcatalogo = 86;

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idtipotransaccion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipotransaccion')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipotransaccion'),
	(select idcampo from tcampo where nombre='idtipotransaccion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipotransaccionsistema')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipotransaccionsistema'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idtipotransaccion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipotransaccion')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipotransaccion'),
	(select idcampo from tcampo where nombre='idtipotransaccion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo'), 2, 'uno a muchos');
/*
insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1263, 84, 1266, 85, 2, 'uno a muchos'),
	(1263, 84, 1279, 86, 2, 'uno a muchos');*/



select * from ttipocatalogo t3 ;
select * from tcatalogo t2 ;
select * from ttipocampo t2 ;
select * from ttiporelacion t ;
select * from tcampo t2 where idcatalogo = 83;

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='iddane' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdane')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdane'),
	(select idcampo from tcampo where nombre='iddanetransaccion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='iddane' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdane')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdane'),
	(select idcampo from tcampo where nombre='iddaneapertura' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproductosistema')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproductosistema'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idmoneda' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimmoneda')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimmoneda'),
	(select idcampo from tcampo where nombre='idmoneda' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idpersona' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona'),
	(select idcampo from tcampo where nombre='idpersona' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproductosistema')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproductosistema'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idpersona' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona'),
	(select idcampo from tcampo where nombre='idpersonacontraparte' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='ipersona' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona'),
	(select idcampo from tcampo where nombre='idpersonatitular' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='factefectivo'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idtipoidentificacion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipoidentificacion')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipoidentificacion'),
	(select idcampo from tcampo where nombre='idtipoidentificacion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidadsistema')),
	(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidadsistema'), 2, 'uno a muchos');

/*
insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1116, 70, 1278, 86, 2, 'uno a muchos'),,
	(1116, 70, 1187, 78, 2, 'uno a muchos'),,
	(1108, 68, 1293, 86, 2, 'uno a muchos'),,
	(1080, 63, 1184, 78, 2, 'uno a muchos'),,
	(1080, 63, 1284, 86, 2, 'uno a muchos'),,
	(1080, 63, 1277, 86, 2, 'uno a muchos'),,
	(1085, 64, 1259, 83, 2, 'uno a muchos');*/

select tcatalogo.nombrecatalogo, tcampo.* 
from tcampo inner join tcatalogo 
on tcampo.idcatalogo = tcatalogo.idcatalogo where idcampo = 1259;


