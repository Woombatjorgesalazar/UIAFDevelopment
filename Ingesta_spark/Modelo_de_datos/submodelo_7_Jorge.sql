select * from public.tproceso t ;
select * from get_proceso('IMPO') as proceso;


SELECT
       *
       
FROM 
       information_schema.table_constraints AS tc 
       JOIN information_schema.key_column_usage AS kcu
         ON tc.constraint_name = kcu.constraint_name
       JOIN information_schema.constraint_column_usage AS ccu
         ON ccu.constraint_name = tc.constraint_name
WHERE constraint_type = 'PRIMARY KEY' AND tc.table_name='tcatalogo';

SELECT
       tc.table_schema, tc.constraint_name, tc.table_name, kcu.column_name, 
       ccu.table_schema AS foreign_table_schema,
       ccu.table_name AS foreign_table_name,
       ccu.column_name AS foreign_column_name
       
FROM 
       information_schema.table_constraints AS tc 
       JOIN information_schema.key_column_usage AS kcu
         ON tc.constraint_name = kcu.constraint_name
       JOIN information_schema.constraint_column_usage AS ccu
         ON ccu.constraint_name = tc.constraint_name
WHERE kcu.column_name = 'idcatalogo';

select * from information_schema.constraint_table_usage where table_name = 'tcampo';

select * from information_schema.key_column_usage kcu where table_name = 'trelacion';


select *
from information_schema.constraint_table_usage as ctu
		join information_schema.key_column_usage as kcu
			on ctu.constraint_name = kcu.constraint_name
where ctu.table_name  = 'tcampo'  and kcu.table_name != 'tcampo';


select * from tcatalogo t ;

select * from tcampo where idcatalogo = 6;


select * from trelacion;


select * from ttipocatalogo t ;

select * from tsistema t2 ;

select * from tcatalogo t ;
select * from ttipocampo t ;

insert into ttipocampo (tipocampo)
values ('llave primaria'),('llave foranea');

insert into ttipocatalogo (tipocatalogo) values ('Dimension');

insert into ttipocatalogo (tipocatalogo) values ('Dimension Conformada');

insert into tsistema (nombre, servidor, basedatos, enlace) 
values ('dwh_uiaf', 'hadoop-hive', 'dwh_uiaf', 'SIN');

<<<<<<< HEAD
insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimpersona', 9, '', 'SIN', 'F')

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (63, 6, 'idpersona', 'int', 0, 1),(63, 7, 'idtipoidentificacion', 'int', 0, 2),
	(63, 6, 'identificacion', 'string', 0, 3), (63, 6, 'nombresrazonsocial', 'string', 0, 4),
	(63, 4, 'fechanacimientocreacion', 'string', 0, 5),
	(63, 7, 'idactividadeconomica', 'int', 0, 6),
	(63, 7, 'idtiposgsss', 'int', 0, 7),
	(63, 7, 'iddaneresidencia', 'string', 0, 8),
	(63, 7, 'iddanenacimiento', 'string', 0, 9),
	(63, 7, 'idroluiaf', 'int', 0, 10);

select * from tcampo t where idcatalogo = '64' ;
	
	
insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimtipoidentificacion', 9, '', 'SIN', 'F');
=======
select idsistema from tsistema where nombre='dwh_uiaf'

select * from tcatalogo

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimpersona', (select idsistema from tsistema where nombre='dwh_uiaf'), 'personas unicas homologadas UIAF', 'SIN', 'F')

select currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo'))
 select * from ttipocampo t 

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values 
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idpersona', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idtipoidentificacion', 'int', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'identificacion', 'string', 0, 3), 
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'nombresrazonsocial', 'string', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'fechanacimientocreacion', 'string', 0, 5),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idactividadeconomica', 'int', 0, 6),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idtiposgsss', 'int', 0, 7),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'iddaneresidencia', 'string', 0, 8),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'iddanenacimiento', 'string', 0, 9),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idroluiaf', 'int', 0, 10);

select * from tcampo t where idcatalogo = 33;
	
	
insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimtipoidentificacion', (select idsistema from tsistema where nombre='dwh_uiaf'), 'tipos identificacion homologados en la UIAF', 'SIN', 'F');
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

select * from tcatalogo t ;

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
<<<<<<< HEAD
values (64, 6, 'idtipoidentificacion', 'int', 0, 1),
	(64, 4, 'tipoidentificacion', 'string', 0, 2),
	(64, 4, 'descripcionidentificacion', 'string', 0, 3);
=======
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idtipoidentificacion', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'tipoidentificacion', 'string', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'descripcionidentificacion', 'string', 0, 3);
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589
	
	
select * from trelacion t2 ;

select * from ttiporelacion t2 ;

select idcampo, nombre, tcampo.idcatalogo, nombrecatalogo from tcampo 
inner join tcatalogo on tcampo.idcatalogo = tcatalogo.idcatalogo 
where nombre = 'idtipoidentificacion';

insert into ttiporelacion (tiporelacion)
values ('uno a muchos');

<<<<<<< HEAD
insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1085, 64, 1081, 63, 2, 'uno a muchos');
=======
dimtipoidentificacion

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idtipoidentificacion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipoidentificacion')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipoidentificacion'), 
(select idcampo from tcampo where nombre='idtipoidentificacion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona'), 2, 'uno a muchos')

/*insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1085, 64, 1081, 63, 2, 'uno a muchos');*/

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idtipoidentificacion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipoidentificacion')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipoidentificacion'), 
(select idcampo from tcampo where nombre='idtipoidentificacion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona'), 2, 'uno a muchos')


>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

select * from trelacion t ;

select tca.nombrecatalogo, tca2.nombrecatalogo, tc.nombre from trelacion tr 
inner join tcampo tc on tr.idcatalogodestino = tc.idcatalogo and tr.idcampodestino = tc.idcampo
inner join tcatalogo tca on tc.idcatalogo = tca.idcatalogo 
inner join tcatalogo tca2 on tr.idcatalogoorigen = tca2.idcatalogo 
where tca.nombrecatalogo = 'dimpersona';

select * from ttipocatalogo t ;
select * from tsistema t ;

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
<<<<<<< HEAD
values (3, 'dimtiposgsss', 9, '', 'SIN', 'F');
=======
values (3, 'dimtiposgsss', (select idsistema from tsistema where nombre='dwh_uiaf'), 'Tipo de afiliasion al sistema general de salud', 'SIN', 'F');

>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589
select * from tcatalogo t ;
select * from ttipocampo t ;
select * from tcampo t2 where idcatalogo = 65;

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
<<<<<<< HEAD
values (65, 6, 'idtiposgsss', 'int', 0, 1),
	(65, 4, 'descripcion', 'string', 0, 2),
	(65, 4, 'tipoafiliado', 'string', 0, 3);

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimtipoidentificacionsistema', 9, '', 'SIN', 'F');

select * from ttipocampo t2 ;
insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (66, 6, 'idtipoidentificacionsistema', 'int', 0, 1),
	(66, 7, 'idtipoidentificacion', 'int', 0, 2),
	(66, 4, 'tipoidentificacion', 'string', 0, 3),
	(66, 4, 'descripcionidentificacion', 'string', 0, 4),
	(66, 4, 'tipoidentificacionsistema', 'string', 0, 5),
	(66, 4, 'descripcionsistema', 'string', 0, 6),
	(66, 7, 'idcatalogo', 'int', 0, 7),
	(66, 7, 'idsistema', 'int', 0, 8),
	(66, 4, 'observaciones', 'string', 0, 9);
=======
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idtiposgsss', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'descripcion', 'string', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'tipoafiliado', 'string', 0, 3);

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimtipoidentificacionsistema', (select idsistema from tsistema where nombre='dwh_uiaf'), 'Tabla de homologacion de tipos de identificaion', 'SIN', 'F');

select * from ttipocampo t2 ;
insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idtipoidentificacionsistema', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idtipoidentificacion', 'int', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'tipoidentificacion', 'string', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'descripcionidentificacion', 'string', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'tipoidentificacionsistema', 'string', 0, 5),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'descripcionsistema', 'string', 0, 6),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idcatalogo', 'int', 0, 7),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsistema', 'int', 0, 8),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'observaciones', 'string', 0, 9);
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

select * from tcatalogo t2 ;

select * from tcampo where idcatalogo = 64;

select * from trelacion t;
select * from ttipocatalogo t ;
select * from ttipocampo t ;

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
<<<<<<< HEAD
values (4, 'dimactividadeconimica', 9, '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (67, 6, 'idactividadeconomica', 'int', 0, 1),
	(67, 4, 'actividadeconomica', 'string', 0, 2),
	(67, 4, 'codigoactividad', 'string', 0, 3);
=======
values (4, 'dimactividadeconimica', (select idsistema from tsistema where nombre='dwh_uiaf'), 'Actividad economica principal de la persona', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idactividadeconomica', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'actividadeconomica', 'string', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'codigoactividad', 'string', 0, 3);
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

select * from tcampo t where idcatalogo = 67;
select * from ttipocatalogo t ;

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
<<<<<<< HEAD
values (4, 'dimmoneda', 9, '', 'SIN', 'F');
=======
values (4, 'dimmoneda', (select idsistema from tsistema where nombre='dwh_uiaf'), 'Monedas homologadas por la UIAF', 'SIN', 'F');
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

select * from tcatalogo t ;

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
<<<<<<< HEAD
values (68, 6, 'idmoneda', 'int', 0, 1),
	(68, 4, 'codigo', 'string', 0, 2),
	(68, 4, 'moneda', 'string', 0, 3);
=======
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idmoneda', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'codigo', 'string', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'moneda', 'string', 0, 3);
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

select * from tcampo t where idcatalogo = 68;

select * from tcatalogo t ;

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
<<<<<<< HEAD
values (4, 'dimpais', 9, '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (69, 6, 'idpais', 'int', 0, 1),
	(69, 7, 'idmoneda', 'int', 0, 2),
	(69, 4, 'codigopais', 'string', 0, 3),
	(69, 4, 'nombre', 'string', 0, 4),
	(69, 4, 'fechamodificacion', 'string', 0, 5);

select * from ttipocampo t ;
select * from tcampo t where idcatalogo = 69;
=======
values (4, 'dimpais', (select idsistema from tsistema where nombre='dwh_uiaf'), '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idpais', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idmoneda', 'int', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'codigopais', 'string', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombre', 'string', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'fechamodificacion', 'string', 0, 5);

select * from ttipocampo t ;
select * from tcampo t where idcatalogo = currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo'));
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589
select * from ttipocatalogo t ;
select * from tcatalogo t ;

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
<<<<<<< HEAD
values (4, 'dimdane', 9, '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (70, 6, 'iddane', 'string', 0, 1),
	(70, 7, 'idpais', 'int', 0, 2),
	(70, 4, 'nombremunicipio', 'string', 0, 3),
	(70, 4, 'nombredepartamento', 'string', 0, 4);
=======
values (4, 'dimdane', (select idsistema from tsistema where nombre='dwh_uiaf'), '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'iddane', 'string', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idpais', 'int', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombremunicipio', 'string', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombredepartamento', 'string', 0, 4);
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

select * from tcampo where idcatalogo = 70;
select * from ttipocatalogo t ;
select * from tcatalogo t ;
select * from ttipocampo t ;

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
<<<<<<< HEAD
values (3, 'dimroluiaf', 9, '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (71, 6, 'idroluiaf', 'int', 0, 1),
	(71, 4, 'roluiaf', 'string', 0, 2);

select * from tcampo t where idcatalogo = 71;
=======
values (3, 'dimroluiaf', (select idsistema from tsistema where nombre='dwh_uiaf'), 'Rol dentro de la transaccion reportada', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idroluiaf', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'roluiaf', 'string', 0, 2);

select * from tcampo t where idcatalogo = currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo'));
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589
select * from tcatalogo t ;
select * from ttipocatalogo t ;
select * from ttipocampo t ;

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
<<<<<<< HEAD
values (4, 'dimpersonasistema', 9, '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (72, 6, 'idpersonasistema', 'int', 0, 1),
	(72, 7, 'idpersona', 'int', 0, 2),
	(72, 7, 'idtipoidentificacion', 'int', 0, 3),
	(72, 4, 'idtipoidentificacionsistema', 'string', 0, 4),
	(72, 7, 'identificacion', 'string', 0, 5),
	(72, 4, 'identificacionsistema', 'string', 0, 6),
	(72, 7, 'nombresrazonsocial', 'string', 0, 7),
	(72, 4, 'nombresrazonsocialsistema', 'string', 0, 8),
	(72, 4, 'fechanacimientocreacion', 'timestamp', 0, 9),
	(72, 4, 'fechanacimientocreaciontransaccion', 'timestamp', 0, 10),
	(72, 7, 'idactividadeconomica', 'int', 0, 11),
	(72, 7, 'idtiposgsss', 'int', 0, 12),
	(72, 4, 'tiposgssssitema', 'string', 0, 13),
	(72, 7, 'iddanevive', 'string', 0, 14),
	(72, 4, 'danevivesistema', 'string', 0, 15),
	(72, 7, 'iddanenacimiento', 'string', 0, 16),
	(72, 4, 'danenacimientosistema', 'string', 0, 17),
	(72, 7, 'idsistema', 'int', 0, 18),
	(72, 7, 'idcatalogo', 'int', 0, 19),
	(72, 7, 'idfechacargue', 'float', 0, 20),
	(72, 7, 'identregasistema', 'int', 0 ,21),
	(72, 7, 'numeroregsistema', 'int', 0, 22),
	(72, 7, 'estado', 'int', 0, 23),
	(72, 7, 'idfechatransaccion', 'int', 0, 24),
	(72, 7, 'idroluiaf', 'int', 0, 25);

select * from tcampo where idcatalogo = 72;

select * from tcatalogo t ;
select * from ttipocampo t ;

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimactividadeconomicasistema', 9, '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (73, 6, 'idactividadeconomicasistema', 'int', 0, 1),
	(73, 7, 'idactividadeconomica', 'int', 0, 2),
	(73, 7, 'idcatalogo', 'int', 0, 3),
	(73, 7, 'idsistema', 'int', 0, 4),
	(73, 4, 'actividadeconomica', 'string', 0, 5),
	(73, 4, 'actividadeconomicasistema', 'string', 0, 6),
	(73, 4, 'codigoactividad', 'string', 0, 7),
	(73, 4, 'codigoactividadsistema', 'string', 0, 8);
=======
values (4, 'dimpersonasistema', (select idsistema from tsistema where nombre='dwh_uiaf'), '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idpersonasistema', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idpersona', 'int', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idtipoidentificacion', 'int', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'idtipoidentificacionsistema', 'string', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'identificacion', 'string', 0, 5),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'identificacionsistema', 'string', 0, 6),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'nombresrazonsocial', 'string', 0, 7),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombresrazonsocialsistema', 'string', 0, 8),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'fechanacimientocreacion', 'timestamp', 0, 9),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'fechanacimientocreaciontransaccion', 'timestamp', 0, 10),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idactividadeconomica', 'int', 0, 11),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idtiposgsss', 'int', 0, 12),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'tiposgssssitema', 'string', 0, 13),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'iddaneresidencia', 'string', 0, 14),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'daneresidenciasistema', 'string', 0, 15),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'iddanenacimiento', 'string', 0, 16),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'danenacimientosistema', 'string', 0, 17),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsistema', 'int', 0, 18),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idcatalogo', 'int', 0, 19),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idfechacargue', 'float', 0, 20),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'identregasistema', 'int', 0 ,21),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'numeroregsistema', 'int', 0, 22),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'estado', 'int', 0, 23),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idfechatransaccion', 'int', 0, 24),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idroluiaf', 'int', 0, 25);

select * from tcampo where idcatalogo = currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo'));

select * from tcatalogo t ;
select * from ttipocampo t ;
insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimactividadeconomica', (select idsistema from tsistema where nombre='dwh_uiaf'), 'Tabla de  las actividades economicas', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values 	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idactividadeconomica', 'int', 0, 1),
		(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'actividadeconomicasistema', 'string', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'codigoactividad', 'string', 0, 3);
	

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimactividadeconomicasistema', (select idsistema from tsistema where nombre='dwh_uiaf'), 'Tabla de homologacion de las actividades economicas', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idactividadeconomicasistema', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idactividadeconomica', 'int', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idcatalogo', 'int', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsistema', 'int', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'actividadeconomica', 'string', 0, 5),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'actividadeconomicasistema', 'string', 0, 6),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'codigoactividad', 'string', 0, 7),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'codigoactividadsistema', 'string', 0, 8);
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

select * from tcampo where idcatalogo = 73;
select * from tcatalogo t ;
select * from ttipocampo t ;

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
<<<<<<< HEAD
values (4, 'dimmonedasistema', 9, '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (74, 6, 'idmonedaistema', 'int', 0, 1),
	(74, 7, 'idmoneda', 'int', 0, 2),
	(74, 4, 'moneda', 'string', 0, 3),
	(74, 4, 'codigo', 'string', 0, 4),
	(74, 7, 'idsistema', 'int', 0, 5),
	(74, 7, 'idcatalogo', 'int', 0, 6),
	(74, 4, 'monedatransaccion', 'string', 0, 7);
=======
values (4, 'dimmonedasistema', (select idsistema from tsistema where nombre='dwh_uiaf'), 'Monedas homologadas por la UIAF', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idmonedaistema', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idmoneda', 'int', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'moneda', 'string', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'codigo', 'string', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsistema', 'int', 0, 5),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idcatalogo', 'int', 0, 6),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'monedatransaccion', 'string', 0, 7);
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

select * from tcampo t where idcatalogo = 74;
select * from tcatalogo t ;
select * from ttipocampo t ;

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
<<<<<<< HEAD
values (4, 'dimdanesistema', 9, '', 'SIN', 'F');


insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (75, 6, 'iddanesistema', 'int', 0, 1),
	(75, 7, 'iddane', 'float', 0, 2),
	(75, 4, 'nombremunicipio', 'string', 0, 3),
	(75, 4, 'nombremunicipiosistema', 'string', 0, 4),
	(75, 4, 'nombredepartamento', 'string', 0, 5),
	(75, 4, 'nombredetapartamentosistema', 'string', 0, 6),
	(75, 4, 'codigodanesistema', 'string', 0, 7),
	(75, 7, 'idpais', 'int', 0, 8),
	(75, 7, 'idsistema', 'int', 0, 9),
	(75, 7, 'idcatalogo', 'int', 0, 10);
=======
values (4, 'dimdanesistema', (select idsistema from tsistema where nombre='dwh_uiaf'), 'Tabla de homologación de la geografia politica', 'SIN', 'F');


insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'iddanesistema', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'iddane', 'float', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombremunicipio', 'string', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombremunicipiosistema', 'string', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombredepartamento', 'string', 0, 5),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombredetapartamentosistema', 'string', 0, 6),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'codigodanesistema', 'string', 0, 7),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idpais', 'int', 0, 8),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsistema', 'int', 0, 9),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idcatalogo', 'int', 0, 10);
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

select * from tcampo t where idcatalogo = 75;

select * from tcatalogo t ;
select * from ttipocatalogo t ;
select * from ttipocampo t ;

insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
<<<<<<< HEAD
values (4, 'dimpaisessistema', 9, '', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (76, 6, 'idpaisessistema', 'int', 0, 1),
	(76, 7, 'idcatalogo', 'int', 0, 2),
	(76, 7, 'idsistema', 'int', 0, 3),
	(76, 7, 'idpais', 'int', 0, 4),
	(76, 7, 'idmoneda', 'int', 0, 5),
	(76, 4, 'codigopais', 'string', 0, 6),
	(76, 4, 'nombre', 'string', 0, 7),
	(76, 4, 'nombresistema', 'string', 0, 8),
	(76, 4, 'moneda', 'string', 0, 9),
	(76, 4, 'fechacarga', 'string', 0, 10),
	(76, 4, 'observaciones', 'string', 0, 11);
=======
values (4, 'dimpaisessistema', (select idsistema from tsistema where nombre='dwh_uiaf'), 'Catalogo de homologacion de paises', 'SIN', 'F');

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 6, 'idpaisessistema', 'int', 0, 1),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idcatalogo', 'int', 0, 2),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idsistema', 'int', 0, 3),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idpais', 'int', 0, 4),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 7, 'idmoneda', 'int', 0, 5),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'codigopais', 'string', 0, 6),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombre', 'string', 0, 7),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'nombresistema', 'string', 0, 8),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'moneda', 'string', 0, 9),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'fechacarga', 'string', 0, 10),
	(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')), 4, 'observaciones', 'string', 0, 11);
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

select * from tcatalogo t where idcatalogo >= 63;
select * from trelacion t where idrelacion >= 58;
select * from tcampo where idcatalogo = 72;
select * from tcatalogo t where  nombrecatalogo = 'dimtipoidentificacionsistema';

<<<<<<< HEAD

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1120, 71, 1146, 72, 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1120, 71, 1092, 63, 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1116, 70, 1090, 63, 2, 'uno a muchos'),
	(1116, 70, 1091, 63, 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1108, 68, 1156, 74, 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1108, 68, 1176, 76, 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1108, 68, 1112, 69, 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1111, 69, 1175, 76, 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1116, 70, 1137, 72, 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1111, 69, 1117, 70, 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1116, 70, 1163, 75, 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1111, 69, 1169, 75, 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1105, 67, 1132, 72, 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1105, 67, 1148, 73, 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1105, 67, 1088, 63, 2, 'uno a muchos');
=======
insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idroluiaf' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimroluiaf')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimroluiaf'), 
(select idcampo from tcampo where nombre='idroluiaf' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema'), 2, 'uno a muchos'),
((select idcampo from tcampo where nombre='idroluiaf' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimroluiaf')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimroluiaf'), 
(select idcampo from tcampo where nombre='idroluiaf' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1120, 71, 1146, 72, 2, 'uno a muchos'),
	   (1120, 71, 1092, 63, 2, 'uno a muchos');
	  
	  select * from tcampo t where idcatalogo=42 and nombre like'%dane%'

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='iddane' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdane')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdane'), 
(select idcampo from tcampo where nombre='iddaneresidencia' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='iddane' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdane')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdane'), 
(select idcampo from tcampo where nombre='iddanenacimiento' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona'), 2, 'uno a muchos');	  
	  
insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1116, 70, 1090, 63, 2, 'uno a muchos'),
	   (1116, 70, 1091, 63, 2, 'uno a muchos');
	  

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idmoneda' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimmoneda')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimmoneda'), 
(select idcampo from tcampo where nombre='idmoneda' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimmonedasistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimmonedasistema'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='idmoneda' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimmoneda')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimmoneda'), 
(select idcampo from tcampo where nombre='idmoneda' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpaisessistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpaisessistema'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='idmoneda' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimmoneda')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimmoneda'), 
(select idcampo from tcampo where nombre='idmoneda' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpais')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpais'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1108, 68, 1156, 74, 2, 'uno a muchos');
		 (1108, 68, 1176, 76, 2, 'uno a muchos');
		(1108, 68, 1112, 69, 2, 'uno a muchos');
	
insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idpais' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpais')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpais'), 
(select idcampo from tcampo where nombre='idpais' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpaisessistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpaisessistema'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='idpais' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpais')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpais'), 
(select idcampo from tcampo where nombre='idpais' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdane')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdane'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='idpais' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpais')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpais'), 
(select idcampo from tcampo where nombre='idpais' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdanesistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdanesistema'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='iddane' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdane')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdane'), 
(select idcampo from tcampo where nombre='iddanenacimiento' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='iddane' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdane')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdane'), 
(select idcampo from tcampo where nombre='iddane' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdanesistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimdanesistema'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1111, 69, 1175, 76, 2, 'uno a muchos');
	(1111, 69, 1117, 70, 2, 'uno a muchos');
	(1111, 69, 1169, 75, 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1116, 70, 1137, 72, 2, 'uno a muchos');
values (1116, 70, 1163, 75, 2, 'uno a muchos');

	
insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idactividadeconomica' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimactividadeconomica')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimactividadeconomica'), 
(select idcampo from tcampo where nombre='idactividadeconomica' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='idactividadeconomica' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimactividadeconomica')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimactividadeconomica'), 
(select idcampo from tcampo where nombre='idactividadeconomica' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimactividadeconomicasistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimactividadeconomicasistema'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='idactividadeconomica' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimactividadeconomica')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimactividadeconomica'), 
(select idcampo from tcampo where nombre='idactividadeconomica' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona'), 2, 'uno a muchos');

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1105, 67, 1132, 72, 2, 'uno a muchos');
values (1105, 67, 1148, 73, 2, 'uno a muchos');
values (1105, 67, 1088, 63, 2, 'uno a muchos');


insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idtiposgsss' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtiposgsss')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtiposgsss'), 
(select idcampo from tcampo where nombre='idtiposgsss' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema'), 2, 'uno a muchos');
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1093, 65, 1133, 72, 2, 'uno a muchos');


<<<<<<< HEAD
=======
insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idpersona' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona'), 
(select idcampo from tcampo where nombre='idpersona' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema'), 2, 'uno a muchos'),
((select idcampo from tcampo where nombre='nombresrazonsocial' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona'), 
(select idcampo from tcampo where nombre='nombresrazonsocial' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='identificacion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona'), 
(select idcampo from tcampo where nombre='identificacion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='idtipoidentificacion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona'), 
(select idcampo from tcampo where nombre='idtipoidentificacion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersonasistema'), 2, 'uno a muchos');

>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1080, 63, 1123, 72, 2, 'uno a muchos'), 
	(1083, 63, 1128, 72, 2, 'uno a muchos'),
	(1082, 63, 1126, 72, 2, 'uno a muchos'),
	(1081, 63, 1124, 72, 2, 'uno a muchos');
	
select * from tcatalogo;

<<<<<<< HEAD

=======
insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idtiposgsss' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtiposgsss')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtiposgsss'), 
(select idcampo from tcampo where nombre='idtiposgsss' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='idtipoidentificacion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipoidentificacion')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipoidentificacion'), 
(select idcampo from tcampo where nombre='idtipoidentificacion' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipoidentificacionsistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimtipoidentificacionsistema'), 2, 'uno a muchos');
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1093, 65, 1089, 63, 2, 'uno a muchos');


insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1085, 64, 1097, 66, 2, 'uno a muchos');

select tca.nombrecatalogo, tca2.nombrecatalogo, tc.nombre from trelacion tr 
inner join tcampo tc on tr.idcatalogodestino = tc.idcatalogo and tr.idcampodestino = tc.idcampo
inner join tcatalogo tca on tc.idcatalogo = tca.idcatalogo 
inner join tcatalogo tca2 on tr.idcatalogoorigen = tca2.idcatalogo 
where tca.nombrecatalogo = 'dimtipoidentificacionsistema';
	
select * from trelacion tr
inner join tcampo tc on tr.idcatalogodestino = tc.idcatalogo and tr.idcampodestino = tc.idcampo 
inner join tcatalogo tca on tc.idcatalogo = tca.idcatalogo 
where tca.nombrecatalogo = 'dimpersona';

select * from consulta_tablas_rel('dimpersona', 'tabla');

select * from consult_columnas('tcampo', 'tabla');
select * from consult_columnas('63', 'idtabla');

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
where torigen.nombrecatalogo = 'dimmoneda' and tdestino.nombrecatalogo = 'dimpaisessistema';

select * from cruze_tablas('dimtipoidentificacionsistema', 'dimpersonasistema');

select * from trelacion;
<<<<<<< HEAD

=======
select * from tcatalogo t ;
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589
select * from ttipocatalogo t ;

select * from tcampo where idcatalogo = 72;

<<<<<<< HEAD
select * from consult_columnas('dimtipoidentificacionsistema', 'tabla') ;

select * from tcampo where idcatalogo = 7;

select * from trelacion t where idcampoorigen = 82;

select * from tsistema t where idsistema = 3 or idsistema = 1;

select * from tcargue t where idcargue = 141;

select * from tcargue t ;

select * from tproceso;

select * from consult_etl('ADRES_BDUDA');

select * from tcatalogo t ;

select * from result_cargue('ADRES_BDUDA', 'tabla');

select * from tcatalogo t ;

select * from consult_columnas('2', 'idtabla') ;


=======
select * from tsistema t ;
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

