select * from tcatalogo t ;
select * from ttipocatalogo t ;
select * from tsistema t ;

<<<<<<< HEAD
insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimrol', 9, '', 'SIN', 'F'),
	(4, 'dimrolsistema', 9, '', 'SIN', 'F'),
	(3, 'dimestadoproducto', 9, '', 'SIN', 'F'),
	(2, 'facttitulares', 9, '', 'SIN', 'F'),
	(2, 'factmovimiento', 9, '', 'SIN', 'F'),
	(4, 'dimtipoproducto', 9, '', 'SIN', 'F'),
	(4, 'dimtipoproductosistema', 9, '', 'SIN', 'F');
=======
insert into ttipocatalogo (tipocatalogo)
values('Dimension'),
('Dimension Conformada')

insert into tsistema (nombre,servidor,basedatos,enlace)
values('dwh_uiaf',	'hadoop-hive',	'dwh_uiaf',	'SIN')


SELECT currval(pg_get_serial_sequence('tsistema', 'idsistema'))



insert into tcatalogo (idtipocatalogo ,nombrecatalogo ,idsistema ,descripcioncatalogo, responsable, separador)
values (4, 'dimrol', currval(pg_get_serial_sequence('tsistema', 'idsistema')), '', 'SIN', 'F'),
	(4, 'dimrolsistema', currval(pg_get_serial_sequence('tsistema', 'idsistema')), '', 'SIN', 'F'),
	(3, 'dimestadoproducto', currval(pg_get_serial_sequence('tsistema', 'idsistema')), '', 'SIN', 'F'),
	(2, 'facttitulares', currval(pg_get_serial_sequence('tsistema', 'idsistema')), '', 'SIN', 'F'),
	(2, 'factmovimiento', currval(pg_get_serial_sequence('tsistema', 'idsistema')), '', 'SIN', 'F'),
	(4, 'dimtipoproducto', currval(pg_get_serial_sequence('tsistema', 'idsistema')), '', 'SIN', 'F'),
	(4, 'dimtipoproductosistema', currval(pg_get_serial_sequence('tsistema', 'idsistema')), '', 'SIN', 'F');
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589

select * from ttipocampo t ;
select * from tcatalogo t ;

<<<<<<< HEAD
select * from tcampo where idcatalogo = 72;

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values (87, 6, 'idrol', 'int', 0, 1),
	(87, 4, 'rol', 'string', 0, 2),
	(88, 6, 'idrolsistema', 'int', 0, 1),
	(88, 7, 'idrol', 'int', 0, 2),
	(88, 4, 'rol', 'string', 0, 3),
	(88, 4, 'rolsistema', 'string', 0, 4),
	(88, 7, 'idsistema', 'int', 0, 5),
	(88, 7, 'idcatalogo', 'int', 0, 6),
	(88, 7, 'idfechacargue', 'int', 0, 7),
	(88, 7, 'identregasistema', 'int', 0, 8),
	(88, 7, 'numeroregsistema', 'int', 0, 9),
	(88, 7, 'estado', 'int', 0, 10),
	(88, 7, 'idfechasistema', 'int', 0, 11),
	(89, 6, 'idestadoproducto', 'int', 0, 1),
	(89, 4, 'estadoproducto', 'string', 0, 2),
	(90, 6, 'idtitular', 'int', 0, 1),
	(90, 7, 'idproducto', 'int', 0, 2),
	(90, 7, 'idrol', 'string', 0, 3),
	(90, 7, 'idpersona', 'int', 0, 4),
	(90, 7, 'idcatalogo', 'int', 0, 5),
	(90, 7, 'idsistema', 'int', 0, 6),
	(90, 7, 'idfechacargue', 'int', 0, 7),
	(90, 7, 'identregasistema', 'int', 0, 8),
	(90, 7, 'numeroregsistema', 'int', 0, 9),
	(90, 7, 'estado', 'int', 0, 10),
	(90, 7, 'idfechasistema', 'int', 0, 11),
	(91, 6, 'idmovimiento', 'int', 0, 1),
	(91, 7, 'idproducto', 'int', 0, 2),
	(91, 7, 'idcatalogo', 'int', 0, 3),
	(91, 7, 'idsistema', 'int', 0, 4),
	(91, 7, 'idsector', 'int', 0, 5),
	(91, 7, 'identidad', 'int', 0, 6),
	(91, 7, 'idfechacorte', 'int', 0, 7),
	(91, 7, 'idfechacargue', 'int', 0, 8),
	(91, 7, 'identregasistema', 'int', 0, 9),
	(91, 7, 'numeroregsistema', 'int', 0, 10),
	(91, 7, 'estado', 'int', 0, 11),
	(91, 7, 'idfechasistema', 'int', 0, 12),
	(91, 7, 'idmoneda', 'int', 0, 13),
	(91, 7, 'numero', 'string', 0, 14),
	(91, 4, 'idestadoproducto', 'int', 0, 15),
	(91, 4, 'numeroproducto', 'string', 0, 16),
	(91, 4, 'numeroradicadosistema', 'int', 0, 17),
	(91, 4, 'numeroentregasistema', 'int', 0, 18),
	(91, 4, 'consecutivosistema', 'int', 0, 19),
	(91, 4, 'entradas', 'float', 0, 20),
	(91, 4, 'salidas', 'float', 0, 21),
	(92, 6, 'idtipoproducto', 'int', 0, 1),
	(92, 4, 'tipoproducto', 'string', 0, 2),
	(93, 6, 'idtipoproductosistema', 'int', 0, 1),
	(93, 7, 'idtipoproducto', 'int', 0, 2),
	(93, 7, 'idfechacargue', 'int', 0, 3),
	(93, 7, 'identregasistema', 'int', 0, 4),
	(93, 7, 'numeroregsistema', 'int', 0, 5),
	(93, 7, 'estado', 'int', 0, 6),
	(93, 7, 'idfechasistema', 'int', 0, 7),
	(93, 4, 'tipoproducto', 'string', 0, 8),
	(93, 4, 'tipoproductosistema', 'string', 0, 9);
=======
insert into ttipocampo	(tipocampo)
values('llave primaria'),
('llave foranea')

select idcatalogo from tcatalogo where nombrecatalogo = 'dimrol';

insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)
values ((select idcatalogo from tcatalogo where nombrecatalogo = 'dimrol'), 6, 'idrol', 'int', 0, 1),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimrol'), 4, 'rol', 'string', 0, 2)
	
insert into tcampo (idcatalogo, idtipocampo, nombre, tipodato, longitud, orden)	
values	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimrolsistema'), 6, 'idrolsistema', 'int', 0, 1),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimrolsistema'), 7, 'idrol', 'int', 0, 2),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimrolsistema'), 4, 'rol', 'string', 0, 3),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimrolsistema'), 4, 'rolsistema', 'string', 0, 4),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimrolsistema'), 7, 'idsistema', 'int', 0, 5),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimrolsistema'), 7, 'idcatalogo', 'int', 0, 6),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimrolsistema'), 7, 'idfechacargue', 'int', 0, 7),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimrolsistema'), 7, 'identregasistema', 'int', 0, 8),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimrolsistema'), 7, 'numeroregsistema', 'int', 0, 9),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimrolsistema'), 7, 'estado', 'int', 0, 10),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimrolsistema'), 7, 'idfechasistema', 'int', 0, 11),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimestadoproducto'), 6, 'idestadoproducto', 'int', 0, 1),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimestadoproducto'), 4, 'estadoproducto', 'string', 0, 2),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'facttitulares'), 6, 'idtitular', 'int', 0, 1),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'facttitulares'), 7, 'idproducto', 'int', 0, 2),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'facttitulares'), 7, 'idrol', 'string', 0, 3),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'facttitulares'), 7, 'idpersona', 'int', 0, 4),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'facttitulares'), 7, 'idcatalogo', 'int', 0, 5),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'facttitulares'), 7, 'idsistema', 'int', 0, 6),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'facttitulares'), 7, 'idfechacargue', 'int', 0, 7),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'facttitulares'), 7, 'identregasistema', 'int', 0, 8),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'facttitulares'), 7, 'numeroregsistema', 'int', 0, 9),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'facttitulares'), 7, 'estado', 'int', 0, 10),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'facttitulares'), 7, 'idfechasistema', 'int', 0, 11),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 6, 'idmovimiento', 'int', 0, 1),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 7, 'idproducto', 'int', 0, 2),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 7, 'idcatalogo', 'int', 0, 3),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 7, 'idsistema', 'int', 0, 4),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 7, 'idsector', 'int', 0, 5),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 7, 'identidad', 'int', 0, 6),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 7, 'idfechacorte', 'int', 0, 7),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 7, 'idfechacargue', 'int', 0, 8),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 7, 'identregasistema', 'int', 0, 9),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 7, 'numeroregsistema', 'int', 0, 10),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 7, 'estado', 'int', 0, 11),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 7, 'idfechasistema', 'int', 0, 12),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 7, 'idmoneda', 'int', 0, 13),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 7, 'numero', 'string', 0, 14),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 4, 'idestadoproducto', 'int', 0, 15),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 4, 'numeroproducto', 'string', 0, 16),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 4, 'numeroradicadosistema', 'int', 0, 17),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 4, 'numeroentregasistema', 'int', 0, 18),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 4, 'consecutivosistema', 'int', 0, 19),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 4, 'entradas', 'float', 0, 20),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'factmovimiento'), 4, 'salidas', 'float', 0, 21),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimtipoproducto'), 6, 'idtipoproducto', 'int', 0, 1),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimtipoproducto'), 4, 'tipoproducto', 'string', 0, 2),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimtipoproductosistema'), 6, 'idtipoproductosistema', 'int', 0, 1),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimtipoproductosistema'), 7, 'idtipoproducto', 'int', 0, 2),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimtipoproductosistema'), 7, 'idfechacargue', 'int', 0, 3),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimtipoproductosistema'), 7, 'identregasistema', 'int', 0, 4),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimtipoproductosistema'), 7, 'numeroregsistema', 'int', 0, 5),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimtipoproductosistema'), 7, 'estado', 'int', 0, 6),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimtipoproductosistema'), 7, 'idfechasistema', 'int', 0, 7),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimtipoproductosistema'), 4, 'tipoproducto', 'string', 0, 8),
	((select idcatalogo from tcatalogo where nombrecatalogo = 'dimtipoproductosistema'), 4, 'tipoproductosistema', 'string', 0, 9);
>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589
	
select * from tcatalogo t ;
select * from ttiporelacion t ;

select t1.* from tcampo t1 inner join tcatalogo t2 on t1.idcatalogo = t2. idcatalogo 
where t2.nombrecatalogo = 'dimproducto';

<<<<<<< HEAD
=======
select idcampo from tcampo where nombre='idrol' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimrol')

insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values ((select idcampo from tcampo where nombre='idrol' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimrol')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimrol'), 
(select idcampo from tcampo where nombre='idrol' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimrolsistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimrolsistema'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='idrol' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimrol')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimrol'), 
(select idcampo from tcampo where nombre='idrol' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='facttitulares')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='facttitulares'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='idmoneda' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimmoneda')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimmoneda'), 
(select idcampo from tcampo where nombre='idmoneda' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='factmovimiento')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='factmovimiento'), 2, 'uno a muchos'),
 
((select idcampo from tcampo where nombre='idfecha' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimfecha')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimfecha'), 
(select idcampo from tcampo where nombre='idfechacorte' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='factmovimiento')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='factmovimiento'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='idestadoproducto' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimestadoproducto')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimestadoproducto'), 
(select idcampo from tcampo where nombre='idestadoproducto' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproducto')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproducto'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='idestadoproducto' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimestadoproducto')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimestadoproducto'), 
(select idcampo from tcampo where nombre='idestadoproducto' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproductosistema')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproductosistema'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='identidad' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimestadoproducto'), 
(select idcampo from tcampo where nombre='identidad' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='factmovimiento')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='factmovimiento'), 2, 'uno a muchos'),
 
((select idcampo from tcampo where nombre='identidad' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimestadoproducto'), 
(select idcampo from tcampo where nombre='identidad' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='factmovimiento')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='factmovimiento'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='idsector' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimentidad')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimestadoproducto'), 
(select idcampo from tcampo where nombre='idsector' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='factmovimiento')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='factmovimiento'), 2, 'uno a muchos'),

((select idcampo from tcampo where nombre='idpersona' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimpersona'), 
(select idcampo from tcampo where nombre='idpersona' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='facttitulares')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='facttitulares'), 2, 'uno a muchos'),


((select idcampo from tcampo where nombre='idproducto' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproducto')),
(select idcatalogo from tcatalogo t2 where nombrecatalogo='dimproducto'), 
(select idcampo from tcampo where nombre='idproducto' and idcatalogo =(select idcatalogo from tcatalogo t2 where nombrecatalogo='facttitulares')), 
(select idcatalogo from tcatalogo t2 where nombrecatalogo='facttitulares'), 2, 'uno a muchos'),


>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589
insert into trelacion (idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion)
values (1302, 87, 1305, 88, 2, 'uno a muchos'),
	(1302, 87, 1319, 90, 2, 'uno a muchos'),
	(1108, 68, 1340, 91, 2, 'uno a muchos'),
	(1201, 79, 1334, 91, 2, 'uno a muchos'),
	(1315, 89, 1299, 77, 2, 'uno a muchos'),
	(1315, 89, 1189, 78, 2, 'uno a muchos'),
	(1217, 82, 1333, 91, 2, 'uno a muchos'),
	(1230, 82, 1332, 91, 2, 'uno a muchos'),
	(1080, 63, 1320, 90, 2, 'uno a muchos'),
	(1294, 77, 1318, 90, 2, 'uno a muchos'),
<<<<<<< HEAD
=======

>>>>>>> 9f4fb963a169fe759ad0ee85a63f3106521c6589
	(1349, 92, 1300, 77, 2, 'uno a muchos'),
	(1349, 92, 1352, 93, 2, 'uno a muchos'),
	(1349, 92, 1197, 78, 2, 'uno a muchos'),
	(1294, 77, 1329, 91, 2, 'uno a muchos'),
	(1294, 77, 1341, 91, 2, 'uno a muchos');








