--
-- PostgreSQL database dump
--

-- Dumped from database version 10.6
-- Dumped by pg_dump version 10.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- Name: get_proceso(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_proceso(p_sigla character varying) RETURNS TABLE(proceso character varying, idestadoproceso integer, idperiodicidad integer, nombrecatalogo character varying, idcatalogo integer, idtipocatalogo integer, origen character, tipocatalogo character varying)
    LANGUAGE plpgsql
    AS $$              
begin
	 return query
 	 select pro.proceso ,PRO.idestadoproceso, PRO.idperiodicidad, 
		cat.nombrecatalogo,cat.idcatalogo, cat.idtipocatalogo ,procat.origen, ttipocatalogo.tipocatalogo 
		from tproceso pro  
		inner join tprocesocatalogo procat
		on pro.idproceso =procat.idproceso 
		inner join tcatalogo cat
		on procat.idproceso = pro.idproceso 
		and procat.idcatalogo =cat.idcatalogo 
		inner join ttipocatalogo 
		on cat.idtipocatalogo =ttipocatalogo.idtipocatalogo 
		where pro.sigla= p_sigla and pro.paso=1;
END;
$$;


ALTER FUNCTION public.get_proceso(p_sigla character varying) OWNER TO postgres;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: tcampo; Type: TABLE; Schema: public; Owner: dbauiaf
--

CREATE TABLE public.tcampo (
    idcampo integer NOT NULL,
    idcatalogo integer NOT NULL,
    idtipocampo integer NOT NULL,
    nombre character varying(200) NOT NULL,
    tipodato character varying(20) NOT NULL,
    longitud integer NOT NULL,
    orden integer
);


ALTER TABLE public.tcampo OWNER TO dbauiaf;

--
-- Name: tcampo_idcampo_seq; Type: SEQUENCE; Schema: public; Owner: dbauiaf
--

CREATE SEQUENCE public.tcampo_idcampo_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tcampo_idcampo_seq OWNER TO dbauiaf;

--
-- Name: tcampo_idcampo_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbauiaf
--

ALTER SEQUENCE public.tcampo_idcampo_seq OWNED BY public.tcampo.idcampo;


--
-- Name: tcargue; Type: TABLE; Schema: public; Owner: dbauiaf
--

CREATE TABLE public.tcargue (
    idcargue integer NOT NULL,
    idproceso integer NOT NULL,
    fechaejecucion timestamp without time zone NOT NULL,
    duracion integer,
    fechafin timestamp without time zone,
    registroscargados integer,
    estado integer NOT NULL,
    observacion character varying(200)
);


ALTER TABLE public.tcargue OWNER TO dbauiaf;

--
-- Name: tcargue_idcargue_seq; Type: SEQUENCE; Schema: public; Owner: dbauiaf
--

CREATE SEQUENCE public.tcargue_idcargue_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tcargue_idcargue_seq OWNER TO dbauiaf;

--
-- Name: tcargue_idcargue_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbauiaf
--

ALTER SEQUENCE public.tcargue_idcargue_seq OWNED BY public.tcargue.idcargue;


--
-- Name: tcatalogo; Type: TABLE; Schema: public; Owner: dbauiaf
--

CREATE TABLE public.tcatalogo (
    idcatalogo integer NOT NULL,
    idtipocatalogo integer NOT NULL,
    nombrecatalogo character varying(100) NOT NULL,
    idsistema integer NOT NULL,
    descripcioncatalogo character varying(200) NOT NULL,
    responsable character varying(50) NOT NULL
);


ALTER TABLE public.tcatalogo OWNER TO dbauiaf;

--
-- Name: tcatalogo_idcatalogo_seq; Type: SEQUENCE; Schema: public; Owner: dbauiaf
--

CREATE SEQUENCE public.tcatalogo_idcatalogo_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tcatalogo_idcatalogo_seq OWNER TO dbauiaf;

--
-- Name: tcatalogo_idcatalogo_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbauiaf
--

ALTER SEQUENCE public.tcatalogo_idcatalogo_seq OWNED BY public.tcatalogo.idcatalogo;


--
-- Name: testadoproceso; Type: TABLE; Schema: public; Owner: dbauiaf
--

CREATE TABLE public.testadoproceso (
    idestadoproceso integer NOT NULL,
    estadoproceso character varying(20) NOT NULL
);


ALTER TABLE public.testadoproceso OWNER TO dbauiaf;

--
-- Name: testadoproceso_idestadoproceso_seq; Type: SEQUENCE; Schema: public; Owner: dbauiaf
--

CREATE SEQUENCE public.testadoproceso_idestadoproceso_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.testadoproceso_idestadoproceso_seq OWNER TO dbauiaf;

--
-- Name: testadoproceso_idestadoproceso_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbauiaf
--

ALTER SEQUENCE public.testadoproceso_idestadoproceso_seq OWNED BY public.testadoproceso.idestadoproceso;


--
-- Name: tparametro; Type: TABLE; Schema: public; Owner: dbauiaf
--

CREATE TABLE public.tparametro (
    idparametro integer NOT NULL,
    idproceso integer NOT NULL,
    parametro character varying(50) NOT NULL,
    descripcion character varying(200) NOT NULL,
    tipo character varying(50) NOT NULL,
    valor character varying(50) NOT NULL
);


ALTER TABLE public.tparametro OWNER TO dbauiaf;

--
-- Name: tparametro_idparametro_seq; Type: SEQUENCE; Schema: public; Owner: dbauiaf
--

CREATE SEQUENCE public.tparametro_idparametro_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tparametro_idparametro_seq OWNER TO dbauiaf;

--
-- Name: tparametro_idparametro_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbauiaf
--

ALTER SEQUENCE public.tparametro_idparametro_seq OWNED BY public.tparametro.idparametro;


--
-- Name: tperiodicidad; Type: TABLE; Schema: public; Owner: dbauiaf
--

CREATE TABLE public.tperiodicidad (
    idperiodicidad integer NOT NULL,
    periodicidad character varying(20) NOT NULL
);


ALTER TABLE public.tperiodicidad OWNER TO dbauiaf;

--
-- Name: tperiodicidad_idperiodicidad_seq; Type: SEQUENCE; Schema: public; Owner: dbauiaf
--

CREATE SEQUENCE public.tperiodicidad_idperiodicidad_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tperiodicidad_idperiodicidad_seq OWNER TO dbauiaf;

--
-- Name: tperiodicidad_idperiodicidad_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbauiaf
--

ALTER SEQUENCE public.tperiodicidad_idperiodicidad_seq OWNED BY public.tperiodicidad.idperiodicidad;


--
-- Name: tproceso; Type: TABLE; Schema: public; Owner: dbauiaf
--

CREATE TABLE public.tproceso (
    idproceso integer NOT NULL,
    idestadoproceso integer NOT NULL,
    idperiodicidad integer NOT NULL,
    proceso character varying(200) NOT NULL,
    paso integer NOT NULL,
    formaejecucion character varying(50) NOT NULL,
    responsable character varying(50) NOT NULL,
    rutaetl character varying(300) NOT NULL,
    plazomaximo integer NOT NULL,
    sigla character varying(5),
    idprocesopadre integer
);


ALTER TABLE public.tproceso OWNER TO dbauiaf;

--
-- Name: tproceso_idproceso_seq; Type: SEQUENCE; Schema: public; Owner: dbauiaf
--

CREATE SEQUENCE public.tproceso_idproceso_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tproceso_idproceso_seq OWNER TO dbauiaf;

--
-- Name: tproceso_idproceso_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbauiaf
--

ALTER SEQUENCE public.tproceso_idproceso_seq OWNED BY public.tproceso.idproceso;


--
-- Name: tprocesocatalogo; Type: TABLE; Schema: public; Owner: dbauiaf
--

CREATE TABLE public.tprocesocatalogo (
    idprocesocatalogo integer NOT NULL,
    idproceso integer NOT NULL,
    idcatalogo integer NOT NULL,
    origen character(1) NOT NULL
);


ALTER TABLE public.tprocesocatalogo OWNER TO dbauiaf;

--
-- Name: tprocesocatalogo_idprocesocatalogo_seq; Type: SEQUENCE; Schema: public; Owner: dbauiaf
--

CREATE SEQUENCE public.tprocesocatalogo_idprocesocatalogo_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tprocesocatalogo_idprocesocatalogo_seq OWNER TO dbauiaf;

--
-- Name: tprocesocatalogo_idprocesocatalogo_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbauiaf
--

ALTER SEQUENCE public.tprocesocatalogo_idprocesocatalogo_seq OWNED BY public.tprocesocatalogo.idprocesocatalogo;


--
-- Name: trelacion; Type: TABLE; Schema: public; Owner: dbauiaf
--

CREATE TABLE public.trelacion (
    idrelacion integer NOT NULL,
    idcampoorigen integer NOT NULL,
    idcatalogoorigen integer NOT NULL,
    idcampodestino integer NOT NULL,
    idcatalogodestino integer NOT NULL,
    idtiporelacion integer NOT NULL,
    relacion character varying(200) NOT NULL
);


ALTER TABLE public.trelacion OWNER TO dbauiaf;

--
-- Name: trelacion_idrelacion_seq; Type: SEQUENCE; Schema: public; Owner: dbauiaf
--

CREATE SEQUENCE public.trelacion_idrelacion_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.trelacion_idrelacion_seq OWNER TO dbauiaf;

--
-- Name: trelacion_idrelacion_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbauiaf
--

ALTER SEQUENCE public.trelacion_idrelacion_seq OWNED BY public.trelacion.idrelacion;


--
-- Name: tsistema; Type: TABLE; Schema: public; Owner: dbauiaf
--

CREATE TABLE public.tsistema (
    idsistema integer NOT NULL,
    nombre character varying(50) NOT NULL,
    servidor character varying(200) NOT NULL,
    basedatos character varying(50) NOT NULL,
    enlace character varying(100) NOT NULL
);


ALTER TABLE public.tsistema OWNER TO dbauiaf;

--
-- Name: tsistema_idsistema_seq; Type: SEQUENCE; Schema: public; Owner: dbauiaf
--

CREATE SEQUENCE public.tsistema_idsistema_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tsistema_idsistema_seq OWNER TO dbauiaf;

--
-- Name: tsistema_idsistema_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbauiaf
--

ALTER SEQUENCE public.tsistema_idsistema_seq OWNED BY public.tsistema.idsistema;


--
-- Name: ttipocampo; Type: TABLE; Schema: public; Owner: dbauiaf
--

CREATE TABLE public.ttipocampo (
    idtipocampo integer NOT NULL,
    tipocampo character varying(20) NOT NULL
);


ALTER TABLE public.ttipocampo OWNER TO dbauiaf;

--
-- Name: ttipocampo_idtipocampo_seq; Type: SEQUENCE; Schema: public; Owner: dbauiaf
--

CREATE SEQUENCE public.ttipocampo_idtipocampo_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ttipocampo_idtipocampo_seq OWNER TO dbauiaf;

--
-- Name: ttipocampo_idtipocampo_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbauiaf
--

ALTER SEQUENCE public.ttipocampo_idtipocampo_seq OWNED BY public.ttipocampo.idtipocampo;


--
-- Name: ttipocatalogo; Type: TABLE; Schema: public; Owner: dbauiaf
--

CREATE TABLE public.ttipocatalogo (
    idtipocatalogo integer NOT NULL,
    tipocatalogo character varying(20) NOT NULL
);


ALTER TABLE public.ttipocatalogo OWNER TO dbauiaf;

--
-- Name: ttipocatalogo_idtipocatalogo_seq; Type: SEQUENCE; Schema: public; Owner: dbauiaf
--

CREATE SEQUENCE public.ttipocatalogo_idtipocatalogo_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ttipocatalogo_idtipocatalogo_seq OWNER TO dbauiaf;

--
-- Name: ttipocatalogo_idtipocatalogo_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbauiaf
--

ALTER SEQUENCE public.ttipocatalogo_idtipocatalogo_seq OWNED BY public.ttipocatalogo.idtipocatalogo;


--
-- Name: ttiporelacion; Type: TABLE; Schema: public; Owner: dbauiaf
--

CREATE TABLE public.ttiporelacion (
    idtiporelacion integer NOT NULL,
    tiporelacion character varying(20) NOT NULL
);


ALTER TABLE public.ttiporelacion OWNER TO dbauiaf;

--
-- Name: ttiporelacion_idtiporelacion_seq; Type: SEQUENCE; Schema: public; Owner: dbauiaf
--

CREATE SEQUENCE public.ttiporelacion_idtiporelacion_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ttiporelacion_idtiporelacion_seq OWNER TO dbauiaf;

--
-- Name: ttiporelacion_idtiporelacion_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbauiaf
--

ALTER SEQUENCE public.ttiporelacion_idtiporelacion_seq OWNED BY public.ttiporelacion.idtiporelacion;


--
-- Name: tcampo idcampo; Type: DEFAULT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tcampo ALTER COLUMN idcampo SET DEFAULT nextval('public.tcampo_idcampo_seq'::regclass);


--
-- Name: tcargue idcargue; Type: DEFAULT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tcargue ALTER COLUMN idcargue SET DEFAULT nextval('public.tcargue_idcargue_seq'::regclass);


--
-- Name: tcatalogo idcatalogo; Type: DEFAULT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tcatalogo ALTER COLUMN idcatalogo SET DEFAULT nextval('public.tcatalogo_idcatalogo_seq'::regclass);


--
-- Name: testadoproceso idestadoproceso; Type: DEFAULT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.testadoproceso ALTER COLUMN idestadoproceso SET DEFAULT nextval('public.testadoproceso_idestadoproceso_seq'::regclass);


--
-- Name: tparametro idparametro; Type: DEFAULT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tparametro ALTER COLUMN idparametro SET DEFAULT nextval('public.tparametro_idparametro_seq'::regclass);


--
-- Name: tperiodicidad idperiodicidad; Type: DEFAULT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tperiodicidad ALTER COLUMN idperiodicidad SET DEFAULT nextval('public.tperiodicidad_idperiodicidad_seq'::regclass);


--
-- Name: tproceso idproceso; Type: DEFAULT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tproceso ALTER COLUMN idproceso SET DEFAULT nextval('public.tproceso_idproceso_seq'::regclass);


--
-- Name: tprocesocatalogo idprocesocatalogo; Type: DEFAULT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tprocesocatalogo ALTER COLUMN idprocesocatalogo SET DEFAULT nextval('public.tprocesocatalogo_idprocesocatalogo_seq'::regclass);


--
-- Name: trelacion idrelacion; Type: DEFAULT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.trelacion ALTER COLUMN idrelacion SET DEFAULT nextval('public.trelacion_idrelacion_seq'::regclass);


--
-- Name: tsistema idsistema; Type: DEFAULT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tsistema ALTER COLUMN idsistema SET DEFAULT nextval('public.tsistema_idsistema_seq'::regclass);


--
-- Name: ttipocampo idtipocampo; Type: DEFAULT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.ttipocampo ALTER COLUMN idtipocampo SET DEFAULT nextval('public.ttipocampo_idtipocampo_seq'::regclass);


--
-- Name: ttipocatalogo idtipocatalogo; Type: DEFAULT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.ttipocatalogo ALTER COLUMN idtipocatalogo SET DEFAULT nextval('public.ttipocatalogo_idtipocatalogo_seq'::regclass);


--
-- Name: ttiporelacion idtiporelacion; Type: DEFAULT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.ttiporelacion ALTER COLUMN idtiporelacion SET DEFAULT nextval('public.ttiporelacion_idtiporelacion_seq'::regclass);


--
-- Data for Name: tcampo; Type: TABLE DATA; Schema: public; Owner: dbauiaf
--

COPY public.tcampo (idcampo, idcatalogo, idtipocampo, nombre, tipodato, longitud, orden) FROM stdin;
1	2	5	Número_entrega	int	10	1
2	2	1	Consecutivo	int	10	2
3	2	1	Sector_Entidad	int	2	3
4	2	1	Tipo_entidad	int	3	4
5	2	1	Codigo_entidad	int	3	5
6	2	1	Fecha_corte	date	10	6
7	2	1	Numero_registros	int	10	7
8	2	5	Número_entrega	int	10	1
9	2	2	Consecutivo	int	10	2
10	2	2	Fecha_transaccion	date	10	3
11	2	2	Valor_transacción	int	20	4
12	2	2	Tipo_Moneda	int	1	5
13	2	2	Codigo_Oficina	string	15	6
14	2	2	Tipo_Producto	int	2	7
15	2	2	Tipo_Transacción	int	1	8
16	2	2	Medio_Transaccion	int	1	9
17	2	2	Numero_cuenta	string	20	10
18	2	2	Tipo_Identificacion_titular	int	2	11
19	2	2	Numero_Identificacion_titular	int	20	12
20	2	2	Primer_Apellido_Titular	string	40	13
21	2	2	Segundo_Apellido_Titular	string	40	14
22	2	2	Primer_Nombre_Titular	string	40	15
23	2	2	Otros_Nombres_Titular	string	40	16
24	2	2	Razon_Social	string	60	17
25	2	2	Código_Departamento_Municipio	int	5	18
26	2	2	Tipo_Identificacion_Realiza	int	2	19
27	2	2	Numero_identificacion_realiza	string	20	20
28	2	2	Primer_Apellido_Realiza	string	40	21
29	2	2	Segundo_Apellido_Realiza	string	40	22
30	2	2	Primer_Nombre_Realiza	string	40	23
31	2	2	Otros_Nombres_Realiza	string	40	24
32	2	3	Consecutivo	int	10	1
33	2	3	Sector_Entidad	int	2	2
34	2	3	Tipo_entidad	int	3	3
35	2	3	Código_entidad	int	3	4
36	2	3	Cantidad_Registros	int	10	5
37	3	4	Número_entrega	int	10	1
38	3	4	Consecutivo	int	10	2
39	3	4	Sector_Entidad	int	2	3
40	3	4	Tipo_entidad	int	3	4
41	3	4	Codigo_entidad	int	3	5
42	3	4	Fecha_corte	date	10	6
43	3	4	Numero_registros	int	10	7
44	4	4	Número_entrega	int	10	1
45	4	4	Consecutivo	int	10	2
46	4	4	Fecha_transaccion	date	10	3
47	4	4	Valor_transacción	int	20	4
48	4	4	Tipo_Moneda	int	1	5
49	4	4	Codigo_Oficina	string	15	6
50	4	4	Tipo_Producto	int	2	7
51	4	4	Tipo_Transacción	int	1	8
52	4	4	Medio_Transaccion	int	1	9
53	4	4	Numero_cuenta	string	20	10
54	4	4	Tipo_Identificacion_titular	int	2	11
55	4	4	Numero_Identificacion_titular	int	20	12
56	4	4	Primer_Apellido_Titular	string	40	13
57	4	4	Segundo_Apellido_Titular	string	40	14
58	4	4	Primer_Nombre_Titular	string	40	15
59	4	4	Otros_Nombres_Titular	string	40	16
60	4	4	Razon_Social	string	60	17
61	4	4	Código_Departamento_Municipio	int	5	18
62	4	4	Tipo_Identificacion_Realiza	int	2	19
63	4	4	Numero_identificacion_realiza	string	20	20
64	4	4	Primer_Apellido_Realiza	string	40	21
65	4	4	Segundo_Apellido_Realiza	string	40	22
66	4	4	Primer_Nombre_Realiza	string	40	23
67	4	4	Otros_Nombres_Realiza	string	40	24
68	5	4	Consecutivo	int	10	1
69	5	4	Sector_Entidad	int	2	2
70	5	4	Tipo_entidad	int	3	3
71	5	4	Código_entidad	int	3	4
72	5	4	Cantidad_Registros	int	10	5
73	6	2	CONSECUTIVO	INT	1	1
74	6	2	ESTAFI	VARCHAR	3	2
75	6	2	FECAFIEPS	VARCHAR	10	3
76	6	2	GENAFI	char	1	4
77	6	2	FECNACAFI	VARCHAR	10	5
78	6	2	PRIAPEAFI	VARCHAR	30	6
79	6	2	SEGAPEAFI	VARCHAR	30	7
80	6	2	PRINOMAFI	VARCHAR	30	8
81	6	2	SEGNOMAFI	VARCHAR	30	9
82	6	2	TIPDOCAFI	char	2	10
83	6	2	NUMDOCAFI	VARCHAR	20	11
84	6	2	grp_fml_cotizante_id	INT	10	12
85	6	2	TIPDOCCOTIPRIN	char	2	13
86	6	2	NUMIDECOTIPRIN	VARCHAR	20	14
87	6	2	CODIGOENTIDAD	VARCHAR	70	15
88	6	2	CODIGOREGIMEN	char	1	16
89	6	2	TIPCOTI	char	1	17
90	6	2	NIVELSISBEN	char	2	18
91	6	2	CODDEPAFI	char	2	19
92	6	2	CODCIUAFI	char	3	20
93	6	2	PARENTEZOPRI	char	2	21
94	6	2	TIPAFI	char	2	22
95	6	2	FECHA_CORTE	VARCHAR	10	23
97	7	4	CONSECUTIVO	INT	1	1
98	7	4	ESTAFI	VARCHAR	3	2
99	7	4	FECAFIEPS	VARCHAR	10	3
100	7	4	GENAFI	char	1	4
101	7	4	FECNACAFI	VARCHAR	10	5
102	7	4	PRIAPEAFI	VARCHAR	30	6
103	7	4	SEGAPEAFI	VARCHAR	30	7
104	7	4	PRINOMAFI	VARCHAR	30	8
105	7	4	SEGNOMAFI	VARCHAR	30	9
106	7	4	TIPDOCAFI	char	2	10
107	7	4	NUMDOCAFI	VARCHAR	20	11
108	7	4	grp_fml_cotizante_id	INT	10	12
109	7	4	TIPDOCCOTIPRIN	char	2	13
110	7	4	NUMIDECOTIPRIN	VARCHAR	20	14
111	7	4	CODIGOENTIDAD	VARCHAR	70	15
112	7	4	CODIGOREGIMEN	char	1	16
113	7	4	TIPCOTI	char	1	17
114	7	4	NIVELSISBEN	char	2	18
115	7	4	CODDEPAFI	char	2	19
116	7	4	CODCIUAFI	char	3	20
117	7	4	PARENTEZOPRI	char	2	21
118	7	4	TIPAFI	char	2	22
119	7	4	FECHA_CORTE	VARCHAR	10	23
120	7	4	FECHA_CARGUE	timestamp	1	24
121	7	4	IDCAEGUE	INT	1	25
\.


--
-- Data for Name: tcargue; Type: TABLE DATA; Schema: public; Owner: dbauiaf
--

COPY public.tcargue (idcargue, idproceso, fechaejecucion, duracion, fechafin, registroscargados, estado, observacion) FROM stdin;
\.


--
-- Data for Name: tcatalogo; Type: TABLE DATA; Schema: public; Owner: dbauiaf
--

COPY public.tcatalogo (idcatalogo, idtipocatalogo, nombrecatalogo, idsistema, descripcioncatalogo, responsable) FROM stdin;
2	1	TE10	2	Reporte Transacciones en Efectivo (CE010)	SIN
3	2	TE10_ENCABEZADO	1	Encabezado de Reporte  Transacciones en Efectivo (CE010)	SIN
4	2	TE10_DETALLES	1	Detalles de Reporte  Transacciones en Efectivo (CE010)	SIN
5	2	TE10_PIE	1	Pie de Reporte  Transacciones en Efectivo (CE010)	SIN
6	1	BDUDA	3	Afilidos al SGSSS	SIN
7	2	ADRES-BDUDA	1	Afilidos al SGSSS	SIN
\.


--
-- Data for Name: testadoproceso; Type: TABLE DATA; Schema: public; Owner: dbauiaf
--

COPY public.testadoproceso (idestadoproceso, estadoproceso) FROM stdin;
1	Activo
2	Inactivo
3	En desarrollo
\.


--
-- Data for Name: tparametro; Type: TABLE DATA; Schema: public; Owner: dbauiaf
--

COPY public.tparametro (idparametro, idproceso, parametro, descripcion, tipo, valor) FROM stdin;
1	1	Untima_ejecucion	Fecha en que se ejecuto completamente de forma correcta	date	02-02-2020
2	1	ruta Archivo	Ruta de la carpeta con los archivos a cargar	string	/mnt/uiafcontenido/
3	2	Untima_ejecucion	Fecha en que se ejecuto completamente de forma correcta	date	02-02-2020
4	2	ruta Archivo	Ruta de la carpeta con los archivos a cargar	string	/mnt/BODEGA DE DATOS/SIN/Convenios/ADRES
\.


--
-- Data for Name: tperiodicidad; Type: TABLE DATA; Schema: public; Owner: dbauiaf
--

COPY public.tperiodicidad (idperiodicidad, periodicidad) FROM stdin;
1	Diario
2	Mensual
3	Trimestral
\.


--
-- Data for Name: tproceso; Type: TABLE DATA; Schema: public; Owner: dbauiaf
--

COPY public.tproceso (idproceso, idestadoproceso, idperiodicidad, proceso, paso, formaejecucion, responsable, rutaetl, plazomaximo, sigla, idprocesopadre) FROM stdin;
1	3	1	Cargue Efectivo (CE010)	1	Manual	Eamadom	//HOME/PROYECTOS	10	TE10	1
2	3	2	Cargue Adres BDUDA	1	Manual	Eamadom	//HOME/PROYECTOS	10	BDUDA	\N
\.


--
-- Data for Name: tprocesocatalogo; Type: TABLE DATA; Schema: public; Owner: dbauiaf
--

COPY public.tprocesocatalogo (idprocesocatalogo, idproceso, idcatalogo, origen) FROM stdin;
1	1	2	O
2	1	3	D
3	1	4	D
4	1	5	D
5	2	6	O
6	2	7	D
\.


--
-- Data for Name: trelacion; Type: TABLE DATA; Schema: public; Owner: dbauiaf
--

COPY public.trelacion (idrelacion, idcampoorigen, idcatalogoorigen, idcampodestino, idcatalogodestino, idtiporelacion, relacion) FROM stdin;
1	2	2	38	3	1	uno a uno
2	3	2	39	3	1	uno a uno
3	4	2	40	3	1	uno a uno
4	5	2	41	3	1	uno a uno
5	6	2	42	3	1	uno a uno
6	7	2	43	3	1	uno a uno
7	9	2	45	4	1	uno a uno
8	10	2	46	4	1	uno a uno
9	11	2	47	4	1	uno a uno
10	12	2	48	4	1	uno a uno
11	13	2	49	4	1	uno a uno
12	14	2	50	4	1	uno a uno
13	15	2	51	4	1	uno a uno
14	16	2	52	4	1	uno a uno
15	17	2	53	4	1	uno a uno
16	18	2	54	4	1	uno a uno
17	19	2	55	4	1	uno a uno
18	20	2	56	4	1	uno a uno
19	21	2	57	4	1	uno a uno
20	22	2	58	4	1	uno a uno
21	23	2	59	4	1	uno a uno
22	24	2	60	4	1	uno a uno
23	25	2	61	4	1	uno a uno
24	26	2	62	4	1	uno a uno
25	27	2	63	4	1	uno a uno
26	28	2	64	4	1	uno a uno
27	29	2	65	4	1	uno a uno
28	30	2	66	4	1	uno a uno
29	31	2	67	4	1	uno a uno
30	32	2	68	5	1	uno a uno
31	33	2	69	5	1	uno a uno
32	34	2	70	5	1	uno a uno
33	35	2	71	5	1	uno a uno
34	36	2	72	5	1	uno a uno
35	73	6	97	7	1	uno a uno
36	74	6	98	7	1	uno a uno
37	75	6	99	7	1	uno a uno
38	76	6	100	7	1	uno a uno
39	77	6	101	7	1	uno a uno
40	78	6	102	7	1	uno a uno
41	79	6	103	7	1	uno a uno
42	80	6	104	7	1	uno a uno
43	81	6	105	7	1	uno a uno
44	82	6	106	7	1	uno a uno
45	83	6	107	7	1	uno a uno
46	84	6	108	7	1	uno a uno
47	85	6	109	7	1	uno a uno
48	86	6	110	7	1	uno a uno
49	87	6	111	7	1	uno a uno
50	88	6	112	7	1	uno a uno
51	89	6	113	7	1	uno a uno
52	90	6	114	7	1	uno a uno
53	91	6	115	7	1	uno a uno
54	92	6	116	7	1	uno a uno
55	93	6	117	7	1	uno a uno
56	94	6	118	7	1	uno a uno
57	95	6	119	7	1	uno a uno
\.


--
-- Data for Name: tsistema; Type: TABLE DATA; Schema: public; Owner: dbauiaf
--

COPY public.tsistema (idsistema, nombre, servidor, basedatos, enlace) FROM stdin;
1	Stage	Hadoop-hive	Stage_uiaf	SIN
2	Sirel	Reportes	//reportes.uiaf.;v.co/UIAFContenido	SIN
3	ADRES	File server	\\\\fileserver\\BODEGA DE DATOS\\SIN\\Convenios\\ADRES	SIN
\.


--
-- Data for Name: ttipocampo; Type: TABLE DATA; Schema: public; Owner: dbauiaf
--

COPY public.ttipocampo (idtipocampo, tipocampo) FROM stdin;
1	cabecera
2	cuerpo
3	pie
4	campo tabla
5	id transaccion
\.


--
-- Data for Name: ttipocatalogo; Type: TABLE DATA; Schema: public; Owner: dbauiaf
--

COPY public.ttipocatalogo (idtipocatalogo, tipocatalogo) FROM stdin;
1	Archivo
2	Tabla
\.


--
-- Data for Name: ttiporelacion; Type: TABLE DATA; Schema: public; Owner: dbauiaf
--

COPY public.ttiporelacion (idtiporelacion, tiporelacion) FROM stdin;
1	Cague uno a uno
\.


--
-- Name: tcampo_idcampo_seq; Type: SEQUENCE SET; Schema: public; Owner: dbauiaf
--

SELECT pg_catalog.setval('public.tcampo_idcampo_seq', 121, true);


--
-- Name: tcargue_idcargue_seq; Type: SEQUENCE SET; Schema: public; Owner: dbauiaf
--

SELECT pg_catalog.setval('public.tcargue_idcargue_seq', 1, false);


--
-- Name: tcatalogo_idcatalogo_seq; Type: SEQUENCE SET; Schema: public; Owner: dbauiaf
--

SELECT pg_catalog.setval('public.tcatalogo_idcatalogo_seq', 7, true);


--
-- Name: testadoproceso_idestadoproceso_seq; Type: SEQUENCE SET; Schema: public; Owner: dbauiaf
--

SELECT pg_catalog.setval('public.testadoproceso_idestadoproceso_seq', 3, true);


--
-- Name: tparametro_idparametro_seq; Type: SEQUENCE SET; Schema: public; Owner: dbauiaf
--

SELECT pg_catalog.setval('public.tparametro_idparametro_seq', 4, true);


--
-- Name: tperiodicidad_idperiodicidad_seq; Type: SEQUENCE SET; Schema: public; Owner: dbauiaf
--

SELECT pg_catalog.setval('public.tperiodicidad_idperiodicidad_seq', 3, true);


--
-- Name: tproceso_idproceso_seq; Type: SEQUENCE SET; Schema: public; Owner: dbauiaf
--

SELECT pg_catalog.setval('public.tproceso_idproceso_seq', 2, true);


--
-- Name: tprocesocatalogo_idprocesocatalogo_seq; Type: SEQUENCE SET; Schema: public; Owner: dbauiaf
--

SELECT pg_catalog.setval('public.tprocesocatalogo_idprocesocatalogo_seq', 6, true);


--
-- Name: trelacion_idrelacion_seq; Type: SEQUENCE SET; Schema: public; Owner: dbauiaf
--

SELECT pg_catalog.setval('public.trelacion_idrelacion_seq', 57, true);


--
-- Name: tsistema_idsistema_seq; Type: SEQUENCE SET; Schema: public; Owner: dbauiaf
--

SELECT pg_catalog.setval('public.tsistema_idsistema_seq', 3, true);


--
-- Name: ttipocampo_idtipocampo_seq; Type: SEQUENCE SET; Schema: public; Owner: dbauiaf
--

SELECT pg_catalog.setval('public.ttipocampo_idtipocampo_seq', 5, true);


--
-- Name: ttipocatalogo_idtipocatalogo_seq; Type: SEQUENCE SET; Schema: public; Owner: dbauiaf
--

SELECT pg_catalog.setval('public.ttipocatalogo_idtipocatalogo_seq', 2, true);


--
-- Name: ttiporelacion_idtiporelacion_seq; Type: SEQUENCE SET; Schema: public; Owner: dbauiaf
--

SELECT pg_catalog.setval('public.ttiporelacion_idtiporelacion_seq', 1, true);


--
-- Name: tcampo pk_tcampo; Type: CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tcampo
    ADD CONSTRAINT pk_tcampo PRIMARY KEY (idcampo, idcatalogo);


--
-- Name: tcargue pk_tcargue; Type: CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tcargue
    ADD CONSTRAINT pk_tcargue PRIMARY KEY (idcargue);


--
-- Name: tcatalogo pk_tcatalogo; Type: CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tcatalogo
    ADD CONSTRAINT pk_tcatalogo PRIMARY KEY (idcatalogo);


--
-- Name: testadoproceso pk_testadoproceso; Type: CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.testadoproceso
    ADD CONSTRAINT pk_testadoproceso PRIMARY KEY (idestadoproceso);


--
-- Name: tparametro pk_tparametro; Type: CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tparametro
    ADD CONSTRAINT pk_tparametro PRIMARY KEY (idparametro);


--
-- Name: tperiodicidad pk_tperiodicidad; Type: CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tperiodicidad
    ADD CONSTRAINT pk_tperiodicidad PRIMARY KEY (idperiodicidad);


--
-- Name: tproceso pk_tproceso; Type: CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tproceso
    ADD CONSTRAINT pk_tproceso PRIMARY KEY (idproceso);


--
-- Name: tprocesocatalogo pk_tprocesocatalogo; Type: CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tprocesocatalogo
    ADD CONSTRAINT pk_tprocesocatalogo PRIMARY KEY (idprocesocatalogo);


--
-- Name: trelacion pk_trelacion; Type: CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.trelacion
    ADD CONSTRAINT pk_trelacion PRIMARY KEY (idrelacion);


--
-- Name: tsistema pk_tsistema; Type: CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tsistema
    ADD CONSTRAINT pk_tsistema PRIMARY KEY (idsistema);


--
-- Name: ttipocampo pk_ttipocampo; Type: CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.ttipocampo
    ADD CONSTRAINT pk_ttipocampo PRIMARY KEY (idtipocampo);


--
-- Name: ttipocatalogo pk_ttipocatalogo; Type: CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.ttipocatalogo
    ADD CONSTRAINT pk_ttipocatalogo PRIMARY KEY (idtipocatalogo);


--
-- Name: ttiporelacion pk_ttiporelacion; Type: CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.ttiporelacion
    ADD CONSTRAINT pk_ttiporelacion PRIMARY KEY (idtiporelacion);


--
-- Name: tcampo fk_tcampo_ref_219_ttipocam; Type: FK CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tcampo
    ADD CONSTRAINT fk_tcampo_ref_219_ttipocam FOREIGN KEY (idtipocampo) REFERENCES public.ttipocampo(idtipocampo);


--
-- Name: tcampo fk_tcampo_ref_86_tcatalog; Type: FK CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tcampo
    ADD CONSTRAINT fk_tcampo_ref_86_tcatalog FOREIGN KEY (idcatalogo) REFERENCES public.tcatalogo(idcatalogo);


--
-- Name: tcargue fk_tcargue_ref_43_tproceso; Type: FK CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tcargue
    ADD CONSTRAINT fk_tcargue_ref_43_tproceso FOREIGN KEY (idproceso) REFERENCES public.tproceso(idproceso);


--
-- Name: tcatalogo fk_tcatalog_ref_22_tsistema; Type: FK CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tcatalogo
    ADD CONSTRAINT fk_tcatalog_ref_22_tsistema FOREIGN KEY (idsistema) REFERENCES public.tsistema(idsistema);


--
-- Name: tcatalogo fk_tcatalog_ref_22_ttipocatalogo; Type: FK CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tcatalogo
    ADD CONSTRAINT fk_tcatalog_ref_22_ttipocatalogo FOREIGN KEY (idtipocatalogo) REFERENCES public.ttipocatalogo(idtipocatalogo);


--
-- Name: tparametro fk_tparamet_ref_52_tproceso; Type: FK CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tparametro
    ADD CONSTRAINT fk_tparamet_ref_52_tproceso FOREIGN KEY (idproceso) REFERENCES public.tproceso(idproceso);


--
-- Name: trelacion fk_trelacio_ref_121_tcampo; Type: FK CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.trelacion
    ADD CONSTRAINT fk_trelacio_ref_121_tcampo FOREIGN KEY (idcampodestino, idcatalogodestino) REFERENCES public.tcampo(idcampo, idcatalogo);


--
-- Name: tprocesocatalogo fk_trelacio_ref_129_tcatalogo; Type: FK CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tprocesocatalogo
    ADD CONSTRAINT fk_trelacio_ref_129_tcatalogo FOREIGN KEY (idcatalogo) REFERENCES public.tcatalogo(idcatalogo);


--
-- Name: tproceso fk_trelacio_ref_129_testadoproceso; Type: FK CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tproceso
    ADD CONSTRAINT fk_trelacio_ref_129_testadoproceso FOREIGN KEY (idestadoproceso) REFERENCES public.testadoproceso(idestadoproceso);


--
-- Name: tproceso fk_trelacio_ref_129_tperiodicidad; Type: FK CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tproceso
    ADD CONSTRAINT fk_trelacio_ref_129_tperiodicidad FOREIGN KEY (idperiodicidad) REFERENCES public.tperiodicidad(idperiodicidad);


--
-- Name: tprocesocatalogo fk_trelacio_ref_129_tproceso; Type: FK CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.tprocesocatalogo
    ADD CONSTRAINT fk_trelacio_ref_129_tproceso FOREIGN KEY (idproceso) REFERENCES public.tproceso(idproceso);


--
-- Name: trelacion fk_trelacio_ref_129_ttiporel; Type: FK CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.trelacion
    ADD CONSTRAINT fk_trelacio_ref_129_ttiporel FOREIGN KEY (idtiporelacion) REFERENCES public.ttiporelacion(idtiporelacion);


--
-- Name: trelacion fk_trelacio_ref_99_tcampo; Type: FK CONSTRAINT; Schema: public; Owner: dbauiaf
--

ALTER TABLE ONLY public.trelacion
    ADD CONSTRAINT fk_trelacio_ref_99_tcampo FOREIGN KEY (idcampoorigen, idcatalogoorigen) REFERENCES public.tcampo(idcampo, idcatalogo);


--
-- PostgreSQL database dump complete
--

