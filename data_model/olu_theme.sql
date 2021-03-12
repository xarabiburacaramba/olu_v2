--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.10
-- Dumped by pg_dump version 12.0

-- Started on 2021-03-09 16:20:30

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 3723 (class 0 OID 74746)
-- Dependencies: 230
-- Data for Name: theme; Type: TABLE DATA; Schema: olu2; Owner: olu
--

INSERT INTO olu2.theme VALUES (1, 'http://inspire.ec.europa.eu/theme/cp', 'Cadastral parcels');
INSERT INTO olu2.theme VALUES (2, 'http://inspire.ec.europa.eu/theme/ps', 'Protected sites');
INSERT INTO olu2.theme VALUES (3, 'http://inspire.ec.europa.eu/theme/lc', 'Land cover');
INSERT INTO olu2.theme VALUES (4, 'http://inspire.ec.europa.eu/theme/lu', 'Land use');
INSERT INTO olu2.theme VALUES (5, 'http://inspire.ec.europa.eu/theme/so', 'Soil');
INSERT INTO olu2.theme VALUES (6, 'http://inspire.ec.europa.eu/theme/pf', 'Production and industrial facilities');
INSERT INTO olu2.theme VALUES (7, 'http://inspire.ec.europa.eu/theme/hb', 'Habitats and biotopes');
INSERT INTO olu2.theme VALUES (8, 'http://inspire.ec.europa.eu/theme/bu', 'Buildings');


--
-- TOC entry 3731 (class 0 OID 0)
-- Dependencies: 229
-- Name: origin_theme_theme_id_seq; Type: SEQUENCE SET; Schema: olu2; Owner: olu
--

SELECT pg_catalog.setval('olu2.origin_theme_theme_id_seq', 8, true);


-- Completed on 2021-03-09 16:20:30

--
-- PostgreSQL database dump complete
--

