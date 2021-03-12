--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.10
-- Dumped by pg_dump version 12.0

-- Started on 2021-03-09 16:19:17


INSERT INTO olu2.origin_dataset_type VALUES (1, 'reference dataset', 'Dataset with reference geometry');
INSERT INTO olu2.origin_dataset_type VALUES (2, 'thematic dataset', 'Dataset with mainly thematic data');
INSERT INTO olu2.origin_dataset_type VALUES (3, 'combined dataset', 'Dataset with with thematic data and reference geometry');
INSERT INTO olu2.origin_dataset_type VALUES (4, 'calculated dataset', 'Dataset with calculated attributes');
INSERT INTO olu2.origin_dataset_type VALUES (99, 'undefined', 'Undefined dataset type');

SELECT pg_catalog.setval('olu2.origin_dataset_type_type_id_seq', 1, false);


-- Completed on 2021-03-09 16:19:17

--
-- PostgreSQL database dump complete
--

