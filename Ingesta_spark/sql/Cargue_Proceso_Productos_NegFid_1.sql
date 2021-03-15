--Productos NegFid 1

\c gestioncargues;

INSERT INTO tproceso (idestadoproceso, idperiodicidad, proceso, paso, formaejecucion, responsable, rutaetl, plazomaximo, sigla)
VALUES(3, 1, 'Cargue Producto NegFid 1 (PN110)', 1, 'Manual', 'Hguerrero', '/home/woombatcg/proyectos/proyectos/', 10, 'PN110');

update tproceso set idprocesopadre =currval(pg_get_serial_sequence('tproceso', 'idproceso'))
where idproceso=currval(pg_get_serial_sequence('tproceso', 'idproceso'));


--insert into tsistema (nombre ,servidor ,basedatos ,enlace )
--values('CCNAL','fileserver','/mnt/BODEGA_DE_DATOS/SIN/Requerimientos/Camara_comersio/','Subdirector SAE atorres')

--insert into tcatalogo (idtipocatalogo,nombrecatalogo ,idsistema ,descripcioncatalogo ,responsable,separador )
--values(1,'CP010',currval(pg_get_serial_sequence('tsistema', 'idsistema')),'Listas de Camaras y comersio','SIN','|')

-- Insert Catalogo

insert into tcatalogo (idtipocatalogo,nombrecatalogo ,idsistema ,descripcioncatalogo ,responsable,separador )
values(1,'PN110',2,'Reporte Productos NegFid Ofrecidos 1 (PN110)','SIN','F');

insert into tprocesocatalogo (idproceso,idcatalogo ,origen )
values(currval(pg_get_serial_sequence('tproceso', 'idproceso')),currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),'O');

insert into tcampo (IDCATALOGO,   IDTIPOCAMPO, NOMBRE,TIPODATO,LONGITUD,ORDEN )
values(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),1,'Consecutivo','bigint',10,1),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),1,'Sector_Entidad','int',2,2),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),1,'Tipo_Entidad','int',3,3),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),1,'Codigo_Entidad','int',3,4),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),1,'Fecha_Corte','date',10,5),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),1,'Numero_Negocios_Reportados','bigint',10,6),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Consecutivo','bigint',10,1),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Codigo_SFC','string',20,2),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Modalidad_Negocio','int',1,3),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Tipo_Producto','int',2,4),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Porcentaje_Administracion','string',4,5),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Nombre_Negocio','string',255,6),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Objeto_Negocio','string',500,7),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Fecha_Apertura','date',10,8),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Municipio_Apertura','int',5,9),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Estado_Negocio','int',1,10),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Entradas','bigint',20,11),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Salidas','bigint',20,12),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),2,'Saldo','bigint',20,13),
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
values(2,'PN110_ENCABEZADO',1,'Encabezado de Reporte Productos NegFid Ofrecidos 1 (PN110)','SIN','F');

insert into tprocesocatalogo (idproceso,idcatalogo ,origen )
values(currval(pg_get_serial_sequence('tproceso', 'idproceso')),currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),'D');

insert into tcampo (IDCATALOGO,   IDTIPOCAMPO, NOMBRE,TIPODATO,LONGITUD,ORDEN )
values(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Consecutivo','bigint',10,1),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Sector_Entidad','int',2,2),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Tipo_Entidad','int',3,3),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Codigo_Entidad','int',3,4),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Fecha_Corte','date',10,5),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Numero_Negocios_Reportados','bigint',10,6);

insert into tcatalogo (idtipocatalogo,nombrecatalogo ,idsistema ,descripcioncatalogo ,responsable,separador )
values(2,'PN110_DETALLES',1,'Detalles de Reporte Productos NegFid Ofrecidos 1 (PN110)','SIN','F');

insert into tprocesocatalogo (idproceso,idcatalogo ,origen )
values(currval(pg_get_serial_sequence('tproceso', 'idproceso')),currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),'D');

insert into tcampo (IDCATALOGO,   IDTIPOCAMPO, NOMBRE,TIPODATO,LONGITUD,ORDEN )
values(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Consecutivo','bigint',10,1),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Codigo_SFC','string',20,2),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Modalidad_Negocio','int',1,3),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Tipo_Producto','int',2,4),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Porcentaje_Administracion','string',4,5),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Nombre_Negocio','string',255,6),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Objeto_Negocio','string',500,7),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Fecha_Apertura','date',10,8),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Municipio_Apertura','int',5,9),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Estado_Negocio','int',1,10),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Entradas','bigint',20,11),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Salidas','bigint',20,12),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Saldo','bigint',20,13);

insert into tcatalogo (idtipocatalogo,nombrecatalogo ,idsistema ,descripcioncatalogo ,responsable,separador )
values(2,'PN110_PIE',1,'Pie de Reporte Productos NegFid Ofrecidos 1 (PN110)','SIN','F');

insert into tprocesocatalogo (idproceso,idcatalogo ,origen )
values(currval(pg_get_serial_sequence('tproceso', 'idproceso')),currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),'D');

insert into tcampo (IDCATALOGO,   IDTIPOCAMPO, NOMBRE,TIPODATO,LONGITUD,ORDEN )
values(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Consecutivo','bigint',10,1),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Sector_Entidad','int',2,2),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Tipo_Entidad','int',3,3),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Codigo_Entidad','int',3,4),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),4,'Numero_Productos_Reportados','bigint',10,5);

select * from tproceso;

SELECT * FROM TCATALOGO;

select * from tsistema;


--delete from tcampo where idcatalogo >= 35;
--delete from tprocesocatalogo where idcatalogo >=35;
--delete from tcatalogo where idcatalogo >= 35;
--delete from tproceso where idproceso = 15;

