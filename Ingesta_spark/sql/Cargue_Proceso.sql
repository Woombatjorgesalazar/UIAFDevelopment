--Dian


select *from tperiodicidad t2 ;
insert into tperiodicidad (periodicidad) values('por demanda')

select * from tproceso t

insert into tproceso (idestadoproceso ,idperiodicidad ,proceso ,paso,formaejecucion, responsable, rutaetl ,plazomaximo ,sigla)
values(3,4,'Camaras Comersio',1,'Manual','Eamadom','//home/proyectos/CArgueDWH/',10,'CCNAL');


update tproceso set idprocesopadre =currval(pg_get_serial_sequence('tproceso', 'idproceso')) 
where idproceso=currval(pg_get_serial_sequence('tproceso', 'idproceso'));





SELECT * FROM TCATALOGO T2

select * from tsistema t2 

insert into tsistema (nombre ,servidor ,basedatos ,enlace )
values('CCNAL','fileserver','/mnt/BODEGA_DE_DATOS/SIN/Requerimientos/Camara_comersio/','Subdirector SAE atorres')

insert into tcatalogo (idtipocatalogo,nombrecatalogo ,idsistema ,descripcioncatalogo ,responsable,separador )
values(1,'CCNAL',currval(pg_get_serial_sequence('tsistema', 'idsistema')),'Listas de Camaras y comersio','SIN','|')

select * from tcatalogo t where idcatalogo =currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo'))

select * from tcampo t where idcatalogo =currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo'))

/*tabla incial ARCHIVO*/


insert into tcampo (IDCATALOGO,   IDTIPOCAMPO, NOMBRE,TIPODATO,LONGITUD,ORDEN ) 
values(14,2,'Cod_Camara','String',100,1),
(14,2,'Camara_de_Comercio','String',100,2),
(14,2,'Matricula','String',100,3),
(14,2,'Razon_Social','String',100,4),
(14,2,'Cod_Clase_Identificacion','String',100,5),
(14,2,'Tipo_Identificacion','String',100,6),
(14,2,'Numero_de_Identificacion','String',100,7),
(14,2,'Digito_Verificacion','String',100,8),
(14,2,'Codigo_Municipio_Comercial','String',100,9),
(14,2,'Municipio_Comercial','String',100,10),
(14,2,'Departamento_Comercial','String',100,11),
(14,2,'Direccion_Comercial','String',100,12),
(14,2,'Telefono_Comercial','String',100,13),
(14,2,'Codigo_Municipio_Fiscal','String',100,14),
(14,2,'Municipio_Fiscal','String',100,15),
(14,2,'Departamento_Fiscal','String',100,16),
(14,2,'Direccion_Fiscal','String',100,17),
(14,2,'Telefono_Fiscal','String',100,18),
(14,2,'Correo_Electronico_Comercial','String',100,19),
(14,2,'Ultimo_Año_Renovado','String',100,20),
(14,2,'Fecha_Matricula','String',100,21),
(14,2,'Codigo_Organizacion_Jurídica','String',100,22),
(14,2,'Organizacion_Jurídica','String',100,23),
(14,2,'Codigo_tipo_Sociedad','String',100,24),
(14,2,'Tipo_Sociedad','String',100,25),
(14,2,'Codigo_Categoria','String',100,26),
(14,2,'Categoria','String',100,27),
(14,2,'Estado','String',100,28),
(14,2,'CIIU_Principal','String',100,29),
(14,2,'CIIU_Secundario','String',100,30),
(14,2,'CIIU_3','String',100,31),
(14,2,'CIIU_4','String',100,32),
(14,2,'Capital_Social','String',100,33),
(14,2,'Empleados','String',100,34),
(14,2,'Beneficio_Ley_1780','String',100,35),
(14,2,'Vendedor_de_Juegos_de_Suerte_y_Azar','String',100,36),
(14,2,'Cantidad_Establecimientos','String',100,37),
(14,2,'Fecha_Renovacion','String',100,38),CCNAL
(14,2,'Fecha_Cancelacion','String',100,39),
(14,2,'Activos','String',100,40),
(14,2,'Utilidad_Perdida_Operacional','String',100,41),
(14,2,'Ingresos_Actividad_Ordinaria','String',100,42),
(14,2,'Otros_Ingresos','String',100,43),
(14,2,'Resultado_periodo','String',100,44),
(14,2,'Num_Identificacion_Representante_Legal','String',10,0,45),
(14,2,'Representante_Legal','String',100,46),
(14,2,'Num_Identificacion_Revisor_Fiscal','String',100,47)
(14,2,'Revisor_Fiscal','String',100,48),
(14,2,'Capital_Social_Nacional_Publico','String',100,49),
(14,2,'Capital_Social_Nacional_Privado','String',100,50),
(14,2,'Capital_Social_Extranjero_Publico','String',100,51),
(14,2,'Capital_Social_Extranjero_Privado','String',100,52),
(14,2,'Clasif_Importador_Exportador','String',100,53),
(14,2,'Fecha_Actualizacion_RUES','String',100,54);




insert into tcampo(IDCATALOGO,   IDTIPOCAMPO, NOMBRE,TIPODATO,LONGITUD,ORDEN )
values(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')) ,2,'IDCARGUE','int',1,7),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')) ,2,'FECHA_CARGUE','date',10,8);



insert into tprocesocatalogo (idproceso,idcatalogo ,origen )
values(currval(pg_get_serial_sequence('tproceso', 'idproceso')),currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),'O')



--Crear catalogo destino

insert into tcatalogo (idtipocatalogo,nombrecatalogo ,idsistema ,descripcioncatalogo ,responsable,separador )
values(2,'CAMARA_COMERSIO',1,'aCHIVO DE CAMARAS DE COMERSIO','SIN','')

select * from tcatalogo t 



insert into tcampo(IDCATALOGO,   IDTIPOCAMPO, NOMBRE,TIPODATO,LONGITUD,ORDEN ) 
select currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')) as itcatalogo,idtipocampo ,nombre,tipodato,longitud ,orden  from tcampo where idcatalogo =14

select * from tcampo

insert into tcampo(IDCATALOGO,   IDTIPOCAMPO, NOMBRE,TIPODATO,LONGITUD,ORDEN )
values(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')) ,2,'IDCARGUE','int',1,55),
(currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')) ,2,'FECHA_CARGUE','date',10,56);


insert into tprocesocatalogo (idproceso,idcatalogo ,origen )
values(currval(pg_get_serial_sequence('tproceso', 'idproceso')),currval(pg_get_serial_sequence('tcatalogo', 'idcatalogo')),'D')

select * from tcampo t 

select * from tprocesocatalogo t;


select * from get_proceso('CCNAL')

select * from tcatalogo where idcatalogo =14

update tcatalogo set idtipocatalogo =2 where idcatalogo =15

--currval(pg_get_serial_sequence('tproceso', 'idproceso'))



--SI ES ARCHIVO SE CREAR SUNPROCESO PARA MARCAR CAD UNO
 
insert into tproceso (idestadoproceso ,idperiodicidad ,proceso ,paso,formaejecucion, responsable, rutaetl ,plazomaximo ,sigla,idprocesopadre )
values(3,4,'Cargue archivos Camara y comersio',2,'Manual','Eamadom','//home/proyectos/CArgueDWH/',10,'CCNAL',10)



select * from public.tsistema

select * from tcatalogo 

select * from public.get_proceso('BDUDA')


select * from public.tcargue where idproceso =2


