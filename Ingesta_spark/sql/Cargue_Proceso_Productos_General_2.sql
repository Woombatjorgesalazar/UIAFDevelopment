--Productos General 2

\c gestioncargues;

select * from tproceso;

INSERT INTO tproceso (idestadoproceso, idperiodicidad, proceso, paso, formaejecucion, responsable, rutaetl, plazomaximo, sigla)
VALUES(3, 1, 'Cargue Producto General 2 (PG210)', 1, 'Manual', 'Hguerrero', '/home/woombatcg/proyectos/proyectos/', 10, 'PG210');

update tproceso set idprocesopadre =currval(pg_get_serial_sequence('tproceso', 'idproceso'))
where idproceso=currval(pg_get_serial_sequence('tproceso', 'idproceso'));


SELECT * FROM TCATALOGO;

select * from tsistema;

--insert into tsistema (nombre ,servidor ,basedatos ,enlace )
--values('CCNAL','fileserver','/mnt/BODEGA_DE_DATOS/SIN/Requerimientos/Camara_comersio/','Subdirector SAE atorres')

--insert into tcatalogo (idtipocatalogo,nombrecatalogo ,idsistema ,descripcioncatalogo ,responsable,separador )
--values(1,'CP010',currval(pg_get_serial_sequence('tsistema', 'idsistema')),'Listas de Camaras y comersio','SIN','|')

-- Insert Catalogo

insert into tcatalogo (idtipocatalogo,nombrecatalogo ,idsistema ,descripcioncatalogo ,responsable,separador )
values(1,'PG210',2,'Reporte Productos Generales Ofrecidos 2 (PG210)','SIN','F');

insert into tprocesocatalogo (idproceso,idcatalogo ,origen )
values(currval(pg_get_serial_sequence('tproceso', 'idproceso')),currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),'O');

insert into tcampo (IDCATALOGO,   IDTIPOCAMPO, NOMBRE,TIPODATO,LONGITUD,ORDEN )
values(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),1,'Consecutivo','bigint',10,1),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),1,'Sector_Entidad','int',2,2),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),1,'Tipo_Entidad','int',3,3),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),1,'Codigo_Entidad','int',3,4),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),1,'Fecha_Corte','date',10,5),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),1,'Numero_Productos_Reportados','bigint',10,6),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Consecutivo','bigint',10,1),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Numero_Producto','string',20,2),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Tipo_Producto','int',2,3),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Rol','int',1,4),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Tipo_Identificacion','int',2,5),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Numero_Identificacion','string',20,6),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Digito_Verificacion','int',2,7),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Nombre','string',255,8),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),3,'Consecutivo','bigint',10,1),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),3,'Sector_Entidad','int',2,2),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),3,'Tipo_Entidad','int',3,3),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),3,'Codigo_Entidad','int',3,4),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),3,'Numero_Productos_Reportados','bigint',10,5),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),5,'Numero_Entrega','bigint',10,1);


--select * from tcatalogo t where idcatalogo =currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo'))

--select * from tcampo t where idcatalogo =currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo'))

/*tabla incial ARCHIVO*/


insert into tcatalogo (idtipocatalogo,nombrecatalogo ,idsistema ,descripcioncatalogo ,responsable,separador )
values(2,'PG210_ENCABEZADO',1,'Encabezado de Reporte Productos Generales Ofrecidos 2 (PG210)','SIN','F');

insert into tprocesocatalogo (idproceso,idcatalogo ,origen )
values(currval(pg_get_serial_sequence('tproceso', 'idproceso')),currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),'D');

insert into tcampo (IDCATALOGO,   IDTIPOCAMPO, NOMBRE,TIPODATO,LONGITUD,ORDEN )
values(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Consecutivo','bigint',10,1),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Sector_Entidad','int',2,2),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Tipo_Entidad','int',3,3),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Codigo_Entidad','int',3,4),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Fecha_Corte','date',10,5),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Numero_Productos_Reportados','bigint',10,6);

insert into tcatalogo (idtipocatalogo,nombrecatalogo ,idsistema ,descripcioncatalogo ,responsable,separador )
values(2,'PG210_DETALLES',1,'Detalles de Reporte Productos Generales Ofrecidos 2 (PG210)','SIN','F');

insert into tprocesocatalogo (idproceso,idcatalogo ,origen )
values(currval(pg_get_serial_sequence('tproceso', 'idproceso')),currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),'D');

insert into tcampo (IDCATALOGO,   IDTIPOCAMPO, NOMBRE,TIPODATO,LONGITUD,ORDEN )
values(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Consecutivo','bigint',10,1),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Numero_Producto','string',20,2),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Tipo_Producto','int',2,3),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Rol','int',1,4),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Tipo_Identificacion','int',2,5),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Numero_Identificacion','string',20,6),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Digito_Verificacion','int',2,7),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Nombre','string',255,8);

insert into tcatalogo (idtipocatalogo,nombrecatalogo ,idsistema ,descripcioncatalogo ,responsable,separador )
values(2,'PG210_PIE',1,'Pie de Reporte Productos Generales Ofrecidos 2 (PG210)','SIN','F');

insert into tprocesocatalogo (idproceso,idcatalogo ,origen )
values(currval(pg_get_serial_sequence('tproceso', 'idproceso')),currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),'D');

insert into tcampo (IDCATALOGO,   IDTIPOCAMPO, NOMBRE,TIPODATO,LONGITUD,ORDEN )
values(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Consecutivo','bigint',10,1),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Sector_Entidad','int',2,2),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Tipo_Entidad','int',3,3),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Codigo_Entidad','int',3,4),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Numero_Productos_Reportados','bigint',10,5);
