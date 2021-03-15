insert into dwh_uiaf.dimtipotransaccion (idtipotransaccion, tipotransaccion)
values (1, 'Retiro (dinero en efectivo que entrega la entidad)'),
    (2, 'Deposito (dinero en efectivo que recibe la entidad)');
    
insert into dwh_uiaf.dimtipoproducto (idtipoproducto, tipoproducto)
values (1, 'Noap'),
    (2, 'Noho'),
    (3, 'Nore'),
    (4, 'Error'),
    (11, 'Cuenta de ahorros'),
    (12, 'Cuenta corriente'),
    (13, 'Deposito electronico'),
    (14, 'Certificado de deposito a termino-CDT'),
    (15, 'Certificado de deposito de ahorro a termino-CDAT'),
    (16, 'Certificado de deposito de mercancia-CDM'),
    (17, 'Otras captaciones (diferentes a las anteriores captaciones registradas en los codigos 01 a 06)'),
    (18, 'Credito de consumo'),
    (19, 'Credito comercial'),
    (20, 'Credito de vivienda'),
    (21, 'Microcredito'),
    (22, 'Tarjeta de credito'),
    (23, 'Tarjeta prepago'),
    (24, 'Leasing'),
    (25, 'Factoring'),
    (26, 'Descuento (creditos y o leasing de descuento)'),
    (27, 'Otras colocaciones (diferentes a las anteriores colocaciones registradas en los codigos 08 a 16)'),
    (28, 'Aval y o garantia'),
    (29, 'Aceptacion'),
    (30, 'Operacion de remate y subasta publica'),
    (31, 'Carta de credito de importacion'),
    (32, 'Carta de credito de exportacion'),
    (33, 'Administracion de portafolios de terceros-APT'),
    (34, 'Administracion de valores'),
    (35, 'Derivado - acuerdo de compra por parte del cliente a la entidad reportante'),
    (36, 'Derivado - acuerdo de venta por parte del cliente a la entidad reportante'),
    (37, 'Operacion de contado - compra de titulos o moneda COP por parte del cliente a la entidad reportante'),
    (38, 'Operacion de contado - venta de titulos o moneda COP por parte del cliente a la entidad reportante'),
    (39, 'Fondo de inversion colectiva de tipo general abierto'),
    (40, 'Fondo de inversion colectiva de tipo general cerrado'),
    (41, 'Fondo de inversion colectiva del mercado monetario abierto'),
    (42, 'Fondo de inversion colectiva del mercado monetario cerrado'),
    (43, 'Fondo de inversion colectiva inmobiliario abierto'),
    (44, 'Fondo de inversion colectiva inmobiliario cerrado'),
    (45, 'Fondo de inversion colectiva bursatil abierto'),
    (46, 'Fondo de inversion colectiva bursatil cerrado'),
    (47, 'Cuenta omnibus'),
    (48, 'Fondo de capital privado'),
    (49, 'Fondos mutuos de inversion'),
    (50, 'Fondo de inversion de capital extranjero'),
    (51, 'Fondo de pensiones voluntarias'),
    (52, 'Fondo de cesantia de persona no dependiente'),
    (53, 'Otro tipo de fondo o cuenta (diferente a los registrados en los codigos 29 a 42)'),
    (54, 'Fiducia de inversion - fideicomisos de inversion con destinacion especifica'),
    (55, 'Fiducia de inversion - administracion de inversiones de fondos mutuos de inversion'),
    (56, 'Fiducia inmobiliaria - administracion y pagos'),
    (57, 'Fiducia inmobiliaria - preventas'),
    (58, 'Fiducia inmobiliaria - tesoreria'),
    (59, 'Fiducia de administracion - administracion y pagos'),
    (60, 'Fiducia de administracion - administracion de proceso de titularizacion'),
    (61, 'Fiducia de administracion - administracion de cartera'),
    (62, 'Fiducia de administracion - administracion de procesos concursales'),
    (63, 'Fiducia en garantia - fiducia en garantia propiamente dicha'),
    (64, 'Fiducia en garantia - fiducia en garantia y fuentes de pagos'),
    (65, 'Recursos del sistema general de seguridad social y otros - pasivos pensionales'),
    (66, 'Otros recursos de la seguridad social'),
    (67, 'Seguro de vida con ahorro'),
    (68, 'Seguro de pensiones voluntarias'),
    (69, 'Seguro de rentas voluntarias'),
    (70, 'Seguro educativo'),
    (71, 'Seguro de accidentes personales con ahorro'),
    (72, 'Seguro colectivo de vida con ahorro'),
    (73, 'Seguro de vida grupo con ahorro'),
    (74, 'Seguro de BEPS'),
    (75, 'Otro seguro con componente de ahorro'),
    (76, 'Titulo de capitalizacion');


insert into dwh_uiaf.dimrol (idrol, rol)
values (1, 'No aplica'),
    (2, 'No homologa'),
    (3, 'No reporta'),
    (4, 'Error en la fuenta'),
    (10, 'recibe'),
    (11, 'realiza');

alter table dwh_uiaf.factefectivo change identregatransaccional identregasistema int;
alter table dwh_uiaf.dimtipotransaccionsistema change idtipotransaccionsistma idtipotransaccionsistema int;

alter table dwh_uiaf.dimentidadsistema add columns (identificacionentidad string);


alter table dwh_uiaf.dimentidadsistema change identificacionsistema identificacionentidadsistema string;

create table dwh_uiaf.dimsectorsistema
(idsectorsistema string, idsistema int, idcatalogo int, idsector int, 
sectorsistema string, nombresector string);

alter table dwh_uiaf.dimsectorsistema add columns (nombresectorsistema string);

alter table dwh_uiaf.dimtipoproductosistema add columns (idsistema int, idcatalogo int);

create database dwh_uiaf2;
alter table dwh_uiaf2.dimpersonadetallado change idtipoidentificacionsistema idtipoidentificaciondetallado string;
alter table dwh_uiaf2.dimtipoidentificaciondetallado change idtipoidentificacionsistema idtipoidentificaciondetallado int;
alter table dwh_uiaf2.dimtipoidentificaciondetallado add columns (idtipoidentificacionsistema int);
alter table dwh_uiaf2.dimtipoproductodetallado add columns (idtipoproductosistema int);